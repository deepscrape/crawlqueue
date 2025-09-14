# Resource Monitor Class
import asyncio
import json
import os
import time
import uuid
import firebase_admin.firestore
from narwhals import Unknown
import psutil
from redisCache import redis
from schemas import CoreMetrics, OperationResult, ResourceStatus, SystemStats, TaskStatus
from typing import Optional

MAX_QUEUE_LENGTH = 180
RATE_LIMIT_TTL = 60  # 1 minute
RATE_LIMIT_COUNT = 75  # 75 operations per minute
WAITING_TIME = 0.1  # 5 seconds
MAX_CONCURRENT_JOBS = 10  # Maximum concurrent jobs per machine

class ResourceMonitor:
    def __init__(self, num_cores=None):
        self.memory_threshold = 0.85  # 85%
        self.cpu_threshold = 0.90  # 90%
        self.error_threshold = 0.15  # 15%
        self.num_cores = num_cores or psutil.cpu_count()

    async def check_resources(self):
        memory_usage = psutil.virtual_memory().percent / 100
        cpu_usage = psutil.cpu_percent() / 100
        per_core_usage = psutil.cpu_percent(percpu=True)
        
        resource_status = ResourceStatus(
            memory_ok=memory_usage < self.memory_threshold,
            cpu_ok=cpu_usage < self.cpu_threshold,
            memory_usage=memory_usage,
            cpu_usage=cpu_usage,
            per_core_usage=per_core_usage,
        )

        return resource_status

    async def get_core_metrics(self):
        cores_metrics: list[dict[str, Unknown]] = []
        for core_id, core_usage in enumerate(psutil.cpu_percent(percpu=True)):
            core_metrics = CoreMetrics(
                core_id=core_id,
                cpu_usage=core_usage / 100,
                memory_usage=psutil.virtual_memory().percent / 100,
                )
            cores_metrics.append(core_metrics.model_dump())
        return cores_metrics


# Enhanced Backpressure Controller
class BackpressureController:
    def __init__(self, machine_id, max_concurrent=15):
        self.machine_id = machine_id
        self.resource_monitor = ResourceMonitor()
        self.max_concurrent = max_concurrent
        self.current_jobs = 0

    async def can_accept_job(self):
        resources = await self.resource_monitor.check_resources()

        # Check local resources
        if not resources.memory_ok or not resources.cpu_ok:
            return False

        # Check current job count
        if self.current_jobs >= self.max_concurrent:
            return False

        # Check global state in redis
        global_load = await self.get_global_load()
        if global_load > 0.85:  # 85% global capacity
            return False

        return True

    async def get_global_load(self):
        all_loads = await redis.hgetall("machine_loads")
        if not all_loads:
            return 0
        return sum(float(load) for load in all_loads.values()) / len(all_loads)


# Worker Pool Management
class WorkerMonitor:
    def __init__(self, worker_id, db = None):
        fly_machine_id = str(os.environ.get("FLY_ALLOC_ID")) or ""
        self.machine_id = f"{fly_machine_id}:process:{str(worker_id)}"

        self.firestore_client = db
        self.metrics = SystemStats(
            cpu_usage=0,
            memory_usage=0,
            queue_length=0,
            error_rate=0,
        )

    async def get_operation_queue_len(self):
        # Get the length of the operation queue
        queue_length = await redis.llen("operation_queue")
        return queue_length

    async def record_operation_start(self, operation_id, start_time):
        await redis.hset(
            f"operation_metrics:{operation_id}",
            values={
                "start_time": start_time,
                "machine_id": self.machine_id,
                "status": TaskStatus.IN_PROGRESS,
            },
        )
    async def record_operation_abort(self, operation_id, duration):
        await redis.hset(
            f"operation_metrics:{operation_id}",
            values={
                "duration": duration,
                "machine_id": self.machine_id,
                "status": TaskStatus.CANCELED,
            },
        )

    async def record_metrics(self, operation_metrics: OperationResult):
        operation_id = operation_metrics.operation_id

        # Store in redis for real-time monitoring
        await redis.hset(
            f"operation_metrics:{operation_id}",
            values=operation_metrics.model_dump(exclude={"operation_id"}),
        )

        # Store in Firestore for historical analysis
        if operation_metrics.status == TaskStatus.COMPLETED:
            
            await self.record_operation()
            if not self.firestore_client:
                return
            
            doc_ref = self.firestore_client.collection("operation_metrics").document(
                operation_id
            )
            doc_ref.set(
                {
                    "timestamp": firebase_admin.firestore.firestore.SERVER_TIMESTAMP,
                    "duration": operation_metrics.duration,
                    "memory_used": operation_metrics.memory_used,
                    "urls_processed": operation_metrics.urls_processed,
                    "machine_id": self.machine_id,
                }
            )
        elif operation_metrics.status == TaskStatus.FAILED:

            # Update the operation status in redis
            await self.record_error(operation_metrics.error)

            if not self.firestore_client:
                return

            doc_ref = self.firestore_client.collection("operation_metrics").document(
                operation_id
            )
            doc_ref.set(
                {
                    "timestamp": firebase_admin.firestore.firestore.SERVER_TIMESTAMP,
                    "error": operation_metrics.error,
                    "duration": operation_metrics.duration,
                    "status": operation_metrics.status,
                    "machine_id": self.machine_id,
                }
            )
        elif operation_metrics.status == TaskStatus.CANCELED:
            
            # Update the operation status in redis
            await redis.hset(
                f"operation_metrics:{operation_id}",
                values={"status": TaskStatus.CANCELED},
            )

            if not self.firestore_client:
                return

            doc_ref = self.firestore_client.collection("operation_metrics").document(
                operation_id
            )
            doc_ref.set(
                {
                    "timestamp": firebase_admin.firestore.firestore.SERVER_TIMESTAMP,
                    "status": TaskStatus.CANCELED,
                    "duration": operation_metrics.duration,
                    "urls_processed": operation_metrics.urls_processed,
                    "machine_id": self.machine_id,
                }
            )


    async def record_error(self, error):
        # Record error in redis
        await redis.hincrby("record_error", self.machine_id, 1)
        print(f"Error recorded for machine {self.machine_id}: {error}")

    async def record_operation(self):
        # Record error in redis
        await redis.hincrby("total_operations", self.machine_id, 1)

    async def get_error_rate(self):
        # Calculate error rate based on operation metrics
        error_count = await redis.hget("record_error", self.machine_id)
        total_operations = await redis.hget("total_operations", self.machine_id)
        if error_count and total_operations:
            return int(error_count) / int(total_operations)
        return 0

    async def update_metrics(self):
        """
        Asynchronously updates the system metrics for the current machine.
        This method gathers the current CPU usage, memory usage, queue length, and error rate,
        then updates the internal metrics attribute with these values. It also stores the metrics
        in a Redis hash for external monitoring or aggregation.
        Side Effects:
            - Updates self.metrics with the latest SystemStats.
            - Persists the metrics to Redis under the key "machine_metrics:{machine_id}".
        Raises:
            Any exceptions raised by the underlying async methods or Redis operations.
        """
        queue_length = await self.get_operation_queue_len()
        systats = SystemStats(
                cpu_usage=psutil.cpu_percent(),
                memory_usage=psutil.virtual_memory().percent,
                queue_length=queue_length,
                error_rate=await self.get_error_rate(),
            )
        self.metrics = systats
        
        # values for upstash, mapping for redis io lab
        await redis.hset(f"machine_metrics:{self.machine_id}", values=self.metrics.model_dump())


class DynamicRateLimiter:
    def __init__(self, base_limit=100, weights=None):
        """
        Initialize the dynamic rate limiter with configurable weights.

        Args:
            base_limit (int): The base rate limit. Defaults to 100.
            weights (dict): Custom weights for different factors.
        """
        self.base_limit = base_limit
        self.resource_monitor = ResourceMonitor()
        self.weights = weights or {
            'cpu': 0.4,
            'memory': 0.3,
            'error': 0.2,
            'queue': 0.1
        }
        # Validate weights sum to 1.0
        if abs(sum(self.weights.values()) - 1.0) > 0.001:
            raise ValueError("Weights must sum to 1.0")
        
    async def calculate_rate_limit(self, metrics: 'Optional[SystemStats]' = None):
        """
        Calculate the dynamic rate limit based on system metrics with weighted factors
        and smoothing.

        Args:
            metrics (SystemStats): An instance of SystemStats containing system metrics.
                If None, will gather current system metrics.

        Returns:
            int: The calculated dynamic rate limit between 10 and base_limit.
        """
        if metrics is None:
            # Only gather metrics if none were provided
            resources = await self.resource_monitor.check_resources()
            metrics = SystemStats(
                cpu_usage=resources.cpu_usage * 100,  # Convert to percentage
                memory_usage=resources.memory_usage * 100,  # Convert to percentage
                queue_length=0,
                error_rate=0.05  # Default error rate
            )

        # Weights for different factors (sum should be 1.0)
        WEIGHTS = self.weights

        # Calculate bounded factors (0.1 to 1.0)
        cpu_factor = max(0.1, min(1.0, 1 - (metrics.cpu_usage / 100)))
        memory_factor = max(0.1, min(1.0, 1 - (metrics.memory_usage / 100)))
        error_factor = max(0.1, min(1.0, pow(1 - metrics.error_rate, 2)))
        queue_factor = max(0.1, min(1.0, 1 - (metrics.queue_length / MAX_QUEUE_LENGTH)))

        # Weighted combination of factors
        dynamic_factor = (
            cpu_factor * WEIGHTS['cpu'] +
            memory_factor * WEIGHTS['memory'] +
            error_factor * WEIGHTS['error'] + 
            queue_factor * WEIGHTS['queue']
        )

        # Calculate limit with smoothing
        dynamic_limit = int(self.base_limit * dynamic_factor)
        
        return max(10, min(dynamic_limit, self.base_limit)) # Never go below 10


# self.machine_id = f"{fly_machine_id}worker:{str(worker_id)}"
# class CoreManager:
#     def __init__(self, num_cores=None):
#         fly_machine_id = str(os.environ.get("FLY_ALLOC_ID")) or ""
#         self.num_cores = num_cores or psutil.cpu_count()
#         self.resource_monitor = ResourceMonitor()
#         self.backpressure_controller = BackpressureController(
#             machine_id=f"{fly_machine_id}worker:{str(uuid.uuid4())}"
#         )
#         self.core_queues = {i: f"core_queue:{i}" for i in range(self.num_cores)}

#     async def distribute_task(self, task):
#         # Get core metrics
#         core_metrics = await self.resource_monitor.get_core_metrics()

#         # Find least loaded core
#         least_loaded_core = min(
#             range(self.num_cores), key=lambda i: core_metrics[i]["cpu_usage"]
#         )

#         # Add task to core-specific queue
#         await redis.rpush(self.core_queues[least_loaded_core], json.dumps(task))
#         return least_loaded_core

#     async def process_core_tasks(self, core_id):
#         monitor = WorkerMonitor(f"core_{core_id}")
#         rate_limiter = DynamicRateLimiter()

#         while True:
#             try:
#                 # Check resources
#                 if not await self.backpressure_controller.can_accept_job():
#                     await asyncio.sleep(WAITING_TIME)
#                     continue

#                 # Get task from core-specific queue
#                 task_data = await redis.lpop(self.core_queues[core_id])
#                 if not task_data:
#                     await asyncio.sleep(WAITING_TIME)
#                     continue

#                 # Process task
#                 task = json.loads(task_data)
#                 """ await process_task_with_monitoring(
#                     task["operationId"], task["author"]["id"], task, task_data, monitor
#                 ) """

#             except Exception as e:
#                 print(f"Error in core {core_id}: {e}")
#                 await asyncio.sleep(WAITING_TIME)


# class AdaptiveQueueManager:
#     def __init__(self, redis_connection):
#         self.redis = redis_connection
#         self.rate_limiter = DynamicRateLimiter()

#     async def manage_queue(self):
#         while True:
#             # Get current rate limit
#             rate_limit = await self.rate_limiter.calculate_rate_limit()

#             # Adjust queue processing based on rate limit
#             tasks = await self.await redis.lrange("operation_queue", 0, rate_limit - 1)

#             # Process tasks
#             for task in tasks:
#                 await self.process_task(task)

#             await asyncio.sleep(60)  # Check and adjust every minute

#     async def process_task(self, task):
#         # Implement task processing logic
#         pass


# class MultiCoreOrchestrator:
#     def __init__(self, num_cores=None, workers_per_core=4):
#         self.num_cores = num_cores or psutil.cpu_count()

#         print(f"Number of cores: {self.num_cores}")
#         self.workers_per_core = workers_per_core
#         self.resource_monitor = ResourceMonitor()
#         self.core_manager = CoreManager(num_cores=self.num_cores)
#         self.rate_limiter = DynamicRateLimiter()
#         self.core_workers = {}  # Track workers per core
#         self.running = False
#         self.worker_metrics = {}  # Track metrics per worker

#     async def start(self):
#         """Start the orchestrator with multiple workers per core"""
#         self.running = True
#         start = time.monotonic()
#         # Start task distributor
#         # asyncio.create_task(self.distribute_queue_tasks())

#         # Create a list of all worker arguments
#         worker_args = []
#         print(self.num_cores)
#         # Initialize workers for each core
#         for core_id in range(self.num_cores):
#             self.core_workers[core_id] = []

#             # Create multiple workers per core
#             for worker_num in range(self.workers_per_core):
#                 worker_id = f"core{core_id}_worker{worker_num}"
#                 worker_args.append((core_id, worker_id))

#             print(worker_args)
#             # # Process all workers using the pool
#             # async with Pool() as pool:
#             #     async for results in pool.map(self.run_core_worker, worker_args):
#             #         pass  # Handle results if necessary
#             # self.core_workers[core_id].append(results)

#             print(f"All done in {time.monotonic() - start} seconds")

#     async def run_core_worker(self, args):
#         """Individual worker process for a specific core"""
#         print(args)
#         core_id, worker_id = args
#         monitor = WorkerMonitor(worker_id)
#         core_queue = f"core_queue:{core_id}"
#         self.worker_metrics[worker_id] = {
#             "tasks_processed": 0,
#             "errors": 0,
#             "last_active": time.time(),
#         }

#         while self.running:
#             try:
#                 # Check system resources
#                 resources = await self.resource_monitor.check_resources()
#                 core_metrics = await self.resource_monitor.get_core_metrics()

#                 if (
#                     resources.cpu_ok
#                     and resources.memory_ok
#                     and core_metrics[core_id]["cpu_usage"] < 0.85
#                 ):
#                     # Get task from core-specific queue
#                     task_data = await redis.lpop(core_queue)
#                     if not task_data:
#                         await asyncio.sleep(WAITING_TIME)
#                         continue

#                     # Process task
#                     task = json.loads(task_data)
#                     try:
#                         """ markdown = await process_task_with_monitoring(
#                             task["operationId"],
#                             task["author"]["uid"],
#                             task,
#                             task_data,
#                             monitor,
#                         ) """
#                         self.worker_metrics[worker_id]["tasks_processed"] += 1
#                         self.worker_metrics[worker_id]["last_active"] = time.time()

#                         # return markdown
#                     except Exception as task_error:
#                         self.worker_metrics[worker_id]["errors"] += 1
#                         print(f"Task error in worker {worker_id}: {task_error}")
#                         # Re-raise to be caught by outer try-except
#                         raise task_error
#                 else:
#                     await asyncio.sleep(WAITING_TIME)

#             except Exception as e:
#                 print(f"Error in worker {worker_id} on core {core_id}: {e}")
#                 await monitor.record_error(str(e))
#                 await asyncio.sleep(WAITING_TIME)

#     async def distribute_queue_tasks(self):
#         """Distribute tasks from main queue to core-specific queues"""
#         while self.running:
#             try:
#                 # Get task from main queue
#                 task_data = await redis.rpop("operation_queue")
#                 if task_data:
#                     # Find least loaded core based on worker metrics and core usage
#                     core_metrics = await self.resource_monitor.get_core_metrics()
#                     core_loads = {}

#                     for core_id in range(self.num_cores):
#                         # Calculate core load based on CPU usage and worker metrics
#                         core_workers = [
#                             w
#                             for w in self.worker_metrics.keys()
#                             if w.startswith(f"core{core_id}")
#                         ]
#                         worker_load = sum(
#                             self.worker_metrics[w]["tasks_processed"]
#                             for w in core_workers
#                         )
#                         core_loads[core_id] = core_metrics[core_id]["cpu_usage"] + (
#                             worker_load * 0.1
#                         )

#                     # Find least loaded core
#                     least_loaded_core = min(core_loads, key=core_loads.get)

#                     # Add to core-specific queue
#                     await redis.rpush(f"core_queue:{least_loaded_core}", task_data)
#                 else:
#                     await asyncio.sleep(WAITING_TIME)

#             except Exception as e:
#                 print(f"Error distributing tasks: {e}")
#                 await asyncio.sleep(WAITING_TIME)

#     async def stop(self):
#         """Gracefully stop all workers"""
#         self.running = False

#         # Cancel all worker tasks
#         for core_id in self.core_workers:
#             for worker_task in self.core_workers[core_id]:
#                 worker_task.cancel()
