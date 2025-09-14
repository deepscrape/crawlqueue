import asyncio

from pydantic import BaseModel, ConfigDict
from crawl import process_scheduled_tasks, worker
from crawler_pool import close_all, janitor
from monitor import DynamicRateLimiter, WorkerMonitor
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

class AppConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    worker_monitor: WorkerMonitor
    rate_limiter: DynamicRateLimiter

app = AppConfig(
    worker_monitor=WorkerMonitor(0),
    rate_limiter=DynamicRateLimiter(),
)

class AppJanitor:
    def __init__(self):
        self._janitor = None  # Initialize _app_janitor

    async def __aenter__(self):
        try:
            # await get_crawler(BrowserConfig(
            #     extra_args=config["crawler"]["browser"].get("extra_args", []),
            #     **config["crawler"]["browser"].get("kwargs", {}),
            # ))           # warm‑up
            self._janitor = asyncio.create_task(janitor())  # Start janitor here
            return self
        except Exception as e:
            print(f"Error occurred in lifespan startup: {e}")
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._janitor:
            self._janitor.cancel()
        await close_all()
        print("Lifespan shutdown complete.")

# Add jobs to the scheduler
@scheduler.scheduled_job("interval", seconds=18, max_instances=10, coalesce=True)
async def scheduled_job():
    
    monitor: WorkerMonitor = app.worker_monitor
    await monitor.update_metrics()

    # Only process if system is healthy
    if await should_process_tasks(monitor.metrics):
        await process_scheduled_tasks()
    else:
        print("System under load, deferring scheduled tasks")


async def should_process_tasks(metrics):
    return all(
        [
            metrics["cpu_usage"] < 85,
            metrics["memory_usage"] < 85,
            metrics["error_rate"] < 0.15,
        ]
    )


async def exception_handler(exc):
    # Handle exceptions here
    print(f"Exception in worker process: {exc}")


async def startup_event():
    """
    Key Improvements:

    Dynamic rate limiting based on system metrics
    Per-machine monitoring and coordination
    Adaptive queue length limits
    Error rate tracking and backoff
    System health checks before processing

    """

    scheduler.start()

    # worker_args = []

    # worker_args = [i for i in range(NUM_WORKERS)]
    # for i in range(NUM_WORKERS):
    #     asyncio.create_task(worker(i))

    await worker()
    # async with Pool(
    #     loop_initializer=uvloop.new_event_loop, exception_handler=exception_handler
    # ) as pool:
    #     async for results in pool.map(worker, worker_args):
    #         print(await results)
    #         pass  # Handle results if necessary


# Shutdown event to clean up resources
async def shutdown_event():
    scheduler.shutdown()
    async with AppJanitor() as aj:
        await aj.__aexit__(None, None, None)



async def main():
    async with AppJanitor():
        print("Application startup initiated.")
        try:
            await startup_event()
        except Exception as e:
            print(f"Error occurred in startup event: {e}")
        finally:
            await shutdown_event()
            print("Application shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received")
        # asyncio.run(shutdown_event())
    except Exception as e:
        print(f"Error occurred outside startup event: {e}")
        # asyncio.run(shutdown_event())
    finally:
        print("Worker exited.")
