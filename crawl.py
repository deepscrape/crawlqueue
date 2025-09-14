import asyncio
from datetime import datetime
import json
import logging
import os
import time
from typing import Callable, Dict, List, Optional, Tuple

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlResult,
    CrawlerRunConfig,
    DefaultMarkdownGenerator,
    PruningContentFilter,
)
import psutil
from functools import partial
from aiomultiprocess import Pool
from pydantic import BaseModel

# from firestore import FirebaseClient
from crawler_pool import close_all, janitor
from crawlstore import getCrawlMetadata, updateCrawlOperation
from firestore import FirebaseClient
from functions import crawl_operation_status_color, url_to_unique_name
from grafana import ERROR_COUNTER, MEMORY_USAGE, OPERATION_DURATION, QUEUE_SIZE
from monitor import (
    MAX_CONCURRENT_JOBS,
    WAITING_TIME,
    DynamicRateLimiter,
    WorkerMonitor,
)
from schemas import OperationResult, TaskStatus, UserAbortException
from tigris_storage import TigrisBucketResult, upload_markdown
from triggers import (
    basic_crawl_operation,
)
from redisCache import redis
from utils import is_scheduled_time_in_past

""" 
To determine how many operations you can run, let's calculate the memory usage per operation and then divide the total available memory by the memory usage per operation.

Given that:

10 URLs task have a total memory usage of 100MB
Each operation (or task) has a similar memory usage pattern
The total available memory on each machine is 1GB (1024MB)
We can calculate the memory usage per operation as follows:

Memory usage per operation = Total memory usage / Number of operations = 100MB / 10 operations = 10MB per operation

Now, let's calculate how many operations you can run on a single machine with 1GB of memory:

Number of operations per machine = Total available memory / Memory usage per operation = 1024MB (1GB) / 10MB per operation = 102.4 operations per machine

Since you have 2 machines, each with 1GB of memory, you can run a total of:

Total number of operations = Number of operations per machine x Number of machines = 102.4 operations per machine x 2 machines = 204.8 operations

So, approximately 205 operations can be run simultaneously on your 2-machine cluster with 1GB of memory each, assuming each operation has a similar memory usage pattern.

However, to be safe and account for any potential memory spikes or overhead, you might want to consider reducing this number by 10-20% to ensure your machines don't run out of memory. This would put the estimated number of operations at around 164-184 operations. 
"""

BATCH_SIZE = 5  # Change this based on your needs

LUA_SCRIPT = """
local items = redis.call('LRANGE', KEYS[1], 0, ARGV[1] - 1)
if #items > 0 then
    redis.call('LTRIM', KEYS[1], ARGV[1], -1)
end
return items
"""
logger = logging.getLogger(__name__)

# At module level
_firebase_client = None
_db_instance = None

async def get_firebase_client():
    global _firebase_client, _db_instance
    if _firebase_client is None:
        _firebase_client = FirebaseClient()
        _db_instance, _ = _firebase_client.init_firebase()
    return _db_instance

# --- Helper to get memory ---
def _get_memory_mb():
    try:
        return psutil.Process().memory_info().rss / 1024 / 1024  # MB
    except Exception as e:
        logger.warning(f"Could not get memory info: {e}")
        return None

class MarkdownEntry(BaseModel):
    url: str
    storage_metadata: Optional[TigrisBucketResult | None]
    additional_info: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def create_entry(
        cls,
        url: str,
        metadata: Optional[TigrisBucketResult | None],
        info: Optional[str] = None,
    ) -> Tuple[str, Optional[TigrisBucketResult | None], Optional[str]]:
        entry = cls(url=url, storage_metadata=metadata, additional_info=info)
        return (entry.url, entry.storage_metadata, entry.additional_info)


# Create a separate queue for scheduled tasks
scheduled_queue = "scheduled_operation_queue"


async def schedule_task(task, scheduled_at):
    # Add the task to the scheduled queue with the scheduled time
    await redis.zadd(scheduled_queue, {task: scheduled_at})


async def process_scheduled_tasks():
    while True:
        # Get the current time
        now = datetime.now().timestamp() * 1000

        # Get tasks that are ready to be processed from the scheduled re
        tasks = await redis.zrangebyscore(scheduled_queue, 0, now)

        # Process each task
        for task in tasks:
            # Remove the task from the scheduled queue
            await redis.zrem(scheduled_queue, task)

            # Add the task to the operation queue
            await redis.rpush("operation_queue", task)

        # Wait for 1 minute before checking again
        # await asyncio.sleep(RATE_LIMIT_TTL)


async def redis_pop_batch(task_queue_name: str, batch_size: int) -> list[str]:
    """Pop the first N items from a Redis list atomically (async)."""

    pipe = redis.pipeline()
    # Get the first N items
    pipe.lrange(task_queue_name, 0, batch_size - 1)
    # Trim the list by removing the first N items
    pipe.ltrim(task_queue_name, batch_size, -1)
    # Execute the pipeline
    results = await pipe.exec()

    if len(results) >= 2 and results[1] == "OK":
        return results[0]
    else:
        return []


# https://github.com/omnilib/aiomultiprocess
# https://www.dataleadsfuture.com/aiomultiprocess-super-easy-integrate-multiprocessing-asyncio-in-python/
async def worker():
    """
    urls: string[];
    modelAI?: AIModel;
    author: {
        id: string;
        displayName: string;
    }
    created_At: number;
    schedule_At?: number;
    sumPrompt: string;
    name: string
    status: CrawlOperationStatus
    color: string
    metadata?: CrawlConfig

    """
    # worker_id = args
    monitor = WorkerMonitor(0)
    rate_limiter = DynamicRateLimiter()

    # Example usage
    # isfolderExists = await folder_exists("markdown-files")
    # if not isfolderExists:
    #     print(
    #         "⚠️ Folder does not exist, but it will be created automatically on upload."
    #     )

    while True:
        ret = await main_process(monitor=monitor, rate_limiter=rate_limiter)
        if ret is None:
            await asyncio.sleep(WAITING_TIME)
            continue


async def main_process(
    monitor: WorkerMonitor,
    rate_limiter: DynamicRateLimiter,
):
    results = None

    try:
        # Update system metrics
        await monitor.update_metrics()

        # Dynamic queue length limit
        current_limit = await rate_limiter.calculate_rate_limit(monitor.metrics)

        print("current_limit: ", current_limit)

        # Check queue length and system resources
        queue_length = await monitor.get_operation_queue_len()
        if queue_length > current_limit:
            print(f"Queue exceeded limit. Waiting... Current limit: {current_limit}")
            return None

        # Fetch and process task
        task_dataList = await redis_pop_batch("operation_queue", current_limit)

        if not task_dataList:
            return None

        max_concurrent = MAX_CONCURRENT_JOBS

        # Process tasks concurrently
        async with Pool() as pool:
            async for results in pool.map(
                partial(process_task_with_monitoring, max_concurrent), task_dataList
            ):
                print(f"Found {len(task_dataList)} Tasks to crawl")

        # Adaptive rate limiting
        await redis.incr("rate_limit")
        await redis.expire("rate_limit", current_limit)

        return results

    except Exception as e:
        # Error handling with detailed logging
        print(f"Error processing task: {e}")
        return results
        # finally:
        #     await redis.close()


async def process_task_with_monitoring(max_concurrent=3, task_dataList=None):
    print("\n=== Parallel Crawling with Browser Reuse + Memory Check ===")
    print(max_concurrent, os.getpid())

    if not task_dataList:
        raise Exception("No task data provided")

    task_data = task_dataList
    print("task_data: ", task_data)

    # instance containing a JSON document deserialize to a Python object.
    task = json.loads(task_data)

    # set variables
    operation_id = task["operationId"]
    uid = task["uid"]

    # create new firebase client
    # client: FirebaseClient = FirebaseClient()
    start_time: float = time.time()

    # init client firebase
    db = await get_firebase_client()

    # set monitor pid
    monitor = WorkerMonitor(os.getpid(), db)

    from typing import Any

    results: List[Dict[str, Any]] = []

    try:
        # Track memory before operation
        initial_memory = _get_memory_mb() # MB
        
        # Initialize memory usage variables
        mem_delta_mb = None
        peak_mem_mb = initial_memory

        # set the urls and the size of urls
        urls: list[str] = task.get("urls", [])
        sizeOf_urls = len(urls)

        # configure the browser settings
        browser_config = BrowserConfig(
            headless=True,
            verbose=True,  # corrected from 'verbos=False'
            extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
            browser_type="chromium",
            viewport_height=600,
            viewport_width=800,  # Smaller viewport for better performance
        )  # Default browser configuration

        # set default markdownGenerator
        markdown_generator = DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(
                # Lower → more content retained, higher → more content pruned
                threshold=0.45,
                # "fixed" or "dynamic"
                threshold_type="dynamic",
                # Ignore nodes with <5 words
                min_word_threshold=5,
            ),  # In case you need fit_markdown
        )

        # set default Crawler Configuration
        crawl_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

        # Create the crawler instance
        from crawler_pool import get_crawler
        crawler, _ = await get_crawler(browser_config)
        
        # 1. Define the LLM extraction strategy
        # llm_strategy = LLMExtractionStrategy(
        #     provider="openai/gpt-4o-mini",  # e.g. "ollama/llama2"
        #     api_token=os.getenv("OPENAI_API_KEY"),
        #     schema=Product.model_json_schema(),  # Or use model_json_schema()
        #     extraction_type="schema",
        #     instruction="Extract all recipe from the content.",
        #     chunk_token_threshold=1000,
        #     overlap_rate=0.1,
        #     apply_chunking=True,
        #     input_format="markdown",  # or "html", "fit_markdown"
        #     extra_args={"temperature": 0.0, "max_tokens": 800},
        # )

        # Process the operation with your existing function
        results = await processOperation(
            db,
            operation_id,
            urls,
            uid,
            task,
            task_data,
            crawler,
            crawl_config,
            markdown_generator,
            max_concurrent,
            operation_crawl_func=basic_crawl_operation,
            monitor=monitor,
            start_time=start_time,
        )

        # Calculate resource usage
        end_mem_mb = _get_memory_mb() # <--- Get memory after
        end_time = time.time()
        total_time = end_time - start_time

        if initial_memory is not None and end_mem_mb is not None:
            mem_delta_mb = end_mem_mb - initial_memory # <--- Calculate delta
            peak_mem_mb = max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb) # <--- Get peak memory
            logger.info(f"Memory usage: Start: {initial_memory:.2f} MB, End: {end_mem_mb} MB, Delta: {mem_delta_mb:.2f} MB, Peak: {peak_mem_mb:.2f} MB, Total Time: {total_time:.2f}" )

        operResult = OperationResult(
            machine_id=monitor.machine_id,
            operation_id=operation_id,
            end_time=end_time,
            duration=total_time,
            peak_memory=peak_mem_mb,
            memory_used=mem_delta_mb,
            status=TaskStatus.COMPLETED,
            urls_processed=sizeOf_urls,
        )

        # Record Completed operation metrics 
        await monitor.record_metrics(operResult)

        # Update Prometheus metrics if enabled
        OPERATION_DURATION.observe(total_time)
        MEMORY_USAGE.set(psutil.virtual_memory().percent)

        # FIXME: Update operation status CHECK THE RESULTS
        if results:
            # TODO: FIREBASE UPDATE - change to redis update
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": TaskStatus.COMPLETED,
                    "color": crawl_operation_status_color(TaskStatus.COMPLETED),
                    "storage": results,
                },
                db,
            )
        return results

    except Exception as e:
        # Record the end time for error handling
        end_time = time.time() 
        
        # Handle exceptions and record error metrics
        operResult = OperationResult(
            machine_id=monitor.machine_id,
            operation_id=operation_id,
            error=str(e),
            status=TaskStatus.FAILED,
            duration= end_time - start_time,
        )
        # Increment error counter
        ERROR_COUNTER.inc()
        if operation_id and uid:
            print(f"Error updating operation: {e}")

            # Record error metrics
            await monitor.record_metrics(operResult)
            
            # TODO: FIREBASE UPDATE - change to redis update
            # Update the operation status in the fire database
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": TaskStatus.FAILED,
                    "color": crawl_operation_status_color(TaskStatus.FAILED),
                    "error": str(e),
                },
                db,
            )
        raise Exception(e)

    finally:
        # Always update queue metrics
        queue_length = await monitor.get_operation_queue_len()
        QUEUE_SIZE.set(queue_length)


async def processOperation(
    db,
    operation_id,
    urls,
    uid,
    task,
    task_data,
    crawler: AsyncWebCrawler,
    crawl_config,
    markdown_generator,
    max_concurrent,
    operation_crawl_func,
    monitor: WorkerMonitor,
    start_time: float
):
    scheduled_at = task.get("scheduled_At", None)  # or any other default value
    status = task["status"]

    # metadata Id
    metadataId = task["metadataId"]
    metadata = None

    results: List[
        Dict[str, Optional[TigrisBucketResult | None], Optional[str | None]]
    ] = []

    if metadataId:
        metadata = await getCrawlMetadata(metadataId, uid, db)

    # Check the task status
    if status == TaskStatus.STARTED:
        try:
            # TODO: FIREBASE UPDATE - change to redis update
            # Update status to In Progress before processing
            # await updateCrawlOperation(
            #     uid,
            #     operation_id,
            #     {
            #         "status": TaskStatus.IN_PROGRESS,
            #         "color": crawl_operation_status_color(TaskStatus.IN_PROGRESS),
            #         "error": None,
            #     },
            #     db,
            # )
            # If the status is Start, process the task immediately
            # await crawler.start()

            # Start monitoring metrics
            await monitor.record_operation_start(operation_id, start_time)

            # process urls
            results = await process_urls(
                urls,
                max_concurrent,
                crawler,
                crawl_config,
                markdown_generator,
                operation_crawl_func,
            )
        except Exception as e:
            print(f"Error during crawling: {e}")
            raise e
        

    elif status == TaskStatus.SCHEDULED:
        print(metadata)

        # If the status is Scheduled, check if the scheduled time is in the past
        isInPast, scheduled_at_dt = is_scheduled_time_in_past(scheduled_at)
        if isInPast:
            # If the scheduled time is in the past, process the task immediately

            # TODO: FIREBASE UPDATE - change to redis update
            # # Update status to In Progress before processing
            # await updateCrawlOperation(
            #     uid,
            #     operation_id,
            #     {
            #         "status": TaskStatus.IN_PROGRESS,
            #         "color": crawl_operation_status_color(TaskStatus.IN_PROGRESS),
            #         "error": None,
            #     },
            #     db,
            # )
            # Start monitoring metrics
            await monitor.record_operation_start(operation_id, start_time)
            

            # await crawler.start()

            results = await process_urls(
                urls,
                max_concurrent,
                crawler,
                crawl_config,
                markdown_generator,
                operation_crawl_func,
            )
        else:
            # If the scheduled time is in the future, push the task back into the scheduled queue
            await schedule_task(task_data, scheduled_at)
            print(f"Task {operation_id} is scheduled for {scheduled_at_dt}. Waiting...")
            # return results
    else:
        print(f"Unknown status: {status}")
    
    # UserAbortException("Operation canceled by user")

    try:
        print("\nClosing crawler...")
        await crawler.close()
    # except UserAbortException as e:
    #     print(f"User aborted the operation: {e}")
    #     raise e
    except Exception as e:
        raise e

    return results


async def process_urls(
    urls,
    max_concurrent,
    crawler,
    crawl_config,
    markdown_generator,
    operation_crawl_func: Callable[..., CrawlResult],
):
    success_count = 0
    fail_count = 0
    markdowns: list[dict[str, str | dict | None]] = []

    for i in range(0, len(urls), max_concurrent):
        batch = urls[i : i + max_concurrent]
        tasks: List[CrawlResult] = []

        for j, url in enumerate(batch):
            # Unique session_id per concurrent sub-task
            session_id = f"parallel_session_{i + j}"
            result: CrawlResult = operation_crawl_func(
                url, crawler, crawl_config, markdown_generator, session_id
            )
            tasks.append(result)

        # Gather results
        results: List[CrawlResult] = await asyncio.gather(*tasks, return_exceptions=True)

        # Evaluate results
        for url, result in zip(batch, results):
            if isinstance(result, Exception):
                print(f"Error crawling {url}: {result.error_message}")
                fail_count += 1
                markdowns.append(
                    {
                        "url": url,
                        "metadata": None,
                        "error": result.error_message,
                    }
                )
            elif result.success:
                print(f"Successfully crawled {url}")
                storage_metadata: TigrisBucketResult | None = await upload_markdown(
                    result.markdown,
                    "markdown-files",
                    f"report_{url_to_unique_name(result.url)}.md",
                )
                # entry_tuple = MarkdownEntry.create_entry(url, storage_metadata)
                # markdowns.append(entry_tuple)
                metadata = storage_metadata.to_dict() if storage_metadata else None
                markdowns.append(
                    {
                        "url": url,
                        "metadata": metadata,
                        "error": None,
                    }
                )
                success_count += 1
            else:
                fail_count += 1
                markdowns.append(
                    {
                        "url": url,
                        "metadata": None,
                        "error": "Crawl failed unknown reason",
                    }
                )

    return markdowns