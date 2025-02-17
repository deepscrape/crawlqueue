import asyncio
from datetime import datetime
import json
import os
import time

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    DefaultMarkdownGenerator,
    PruningContentFilter,
)
import psutil
from functools import partial
from aiomultiprocess import Pool


# from crawlstore import getCrawlMetadata, updateCrawlOperation

# from firestore import FirebaseClient
from crawlstore import getCrawlMetadata, updateCrawlOperation
from firestore import FirebaseClient
from grafana import ERROR_COUNTER, MEMORY_USAGE, OPERATION_DURATION, QUEUE_SIZE
from monitor import (
    WAITING_TIME,
    DynamicRateLimiter,
    WorkerMonitor,
)
from triggers import (
    basic_crawl_operation,
)
from redisCache import redis

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


async def pop_batch(task_queue_name: str, batch_size: int) -> list[str]:
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

    while True:
        ret = await main_process(monitor=monitor, rate_limiter=rate_limiter)
        if ret is None:
            await asyncio.sleep(WAITING_TIME)
            continue


async def main_process(
    monitor: WorkerMonitor,
    rate_limiter: DynamicRateLimiter,
):
    try:
        # Update system metrics
        await monitor.update_metrics()
        metrics = monitor.metrics

        markdowns = None

        # Dynamic queue length limit
        current_limit = await rate_limiter.calculate_rate_limit(metrics)

        print("current_limit: ", current_limit)

        # Check queue length and system resources
        queue_length = await redis.llen("operation_queue")
        if queue_length > current_limit:
            print(f"Queue exceeded limit. Waiting... Current limit: {current_limit}")
            return None

        # Fetch and process task
        task_dataList = await pop_batch("operation_queue", current_limit)

        if not task_dataList:
            return None

        max_concurrent = 10

        # Process tasks concurrently
        async with Pool() as pool:
            async for markdowns in pool.map(
                partial(process_task_with_monitoring, max_concurrent), task_dataList
            ):
                print(f"Found {len(task_dataList)} URLs to crawl {markdowns}")
            # Adaptive rate limiting
        await redis.incr("rate_limit")
        await redis.expire("rate_limit", current_limit)

        return markdowns

    except Exception as e:
        # Error handling with detailed logging
        print(f"Error processing task: {e}")
        # if operation_id and uid:
        #     print(f"Error updating operation: {e}")
        # await monitor.record_error(str(e))
        #     await updateCrawlOperation(
        #         uid,
        #         operation_id,
        #         {
        #             "status": "Failed",
        #             "color": "red",
        #             "error": str(e),
        #         },
        #     )
        return None
        # finally:
        #     await redis.close()


async def process_task_with_monitoring(max_concurrent=3, task_dataList=None):
    print("\n=== Parallel Crawling with Browser Reuse + Memory Check ===")
    print(max_concurrent, os.getpid())

    # Start monitoring metrics
    start_time = time.time()

    task_data = task_dataList
    print("task_data: ", task_data)

    # instance containing a JSON document deserialize to a Python object.
    task = json.loads(task_data)

    # set variables
    operation_id = task["operationId"]
    uid = task["uid"]

    # create new firebase client
    client: FirebaseClient = FirebaseClient()

    # init client firebase
    db, auth = client.init_firebase()

    # set monitor pid
    monitor = WorkerMonitor(os.getpid(), db)

    try:
        # Track memory before operation
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

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
        crawler = AsyncWebCrawler(config=browser_config)

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
            browser_config,
            crawl_config,
            markdown_generator,
            max_concurrent,
        )

        # Calculate resource usage
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_used = end_memory - initial_memory
        duration = time.time() - start_time

        # Record success metrics
        await monitor.record_metrics(
            {
                "operation_id": operation_id,
                "duration": duration,
                "memory_used": memory_used,
                "status": "success",
                "urls_processed": sizeOf_urls,
            }
        )

        # Update Prometheus metrics if enabled
        OPERATION_DURATION.observe(duration)
        MEMORY_USAGE.set(psutil.virtual_memory().percent)

        # FIXME: Update operation status CHECK THE RESULTS
        if results:
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": "Completed",
                    "color": "blue",
                    "markdown": results,
                },
                db,
            )

            print(results)
        return results

    except Exception as e:
        # Record error metrics
        await monitor.record_metrics(
            {
                "operation_id": operation_id,
                "error": str(e),
                "status": "failed",
                "duration": time.time() - start_time,
            }
        )
        # Increment error counter
        ERROR_COUNTER.inc()
        if operation_id and uid:
            print(f"Error updating operation: {e}")
            await monitor.record_error(str(e))
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": "Failed",
                    "color": "red",
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
    browser_config,
    crawl_config,
    markdown_generator,
    max_concurrent,
):
    scheduled_at = task.get("scheduled_At", None)  # or any other default value
    status = task["status"]
    metadataId = task["metadataId"]
    metadata = None
    markdowns: list[any] = []

    if metadataId:
        metadata = await getCrawlMetadata(metadataId, uid, db)

    # Check the status of the task
    if status == "Start":
        # print(f"db: {db}")
        # Update status to In Progress before processing
        await updateCrawlOperation(
            uid,
            operation_id,
            {
                "status": "In Progress",
                "color": "cyan",
                "error": None,
            },
            db,
        )
        # If the status is Start, process the task immediately
        await crawler.start()

        # process urls
        markdowns = await process_urls(
            urls, max_concurrent, crawler, crawl_config, markdown_generator
        )

    elif status == "Scheduled":
        print(metadata)
        print("Scheduled")
        # If the status is Scheduled, check if the scheduled time is in the past
        scheduled_at_dt = datetime.fromtimestamp(scheduled_at / 1000)
        now = datetime.now()
        if now >= scheduled_at_dt:
            # If the scheduled time is in the past, process the task immediately
            # Update status to In Progress before processing
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": "In Progress",
                    "color": "cyan",
                    "error": None,
                },
                db,
            )
            await crawler.start()
            markdowns = await process_urls(
                urls, max_concurrent, crawler, crawl_config, markdown_generator
            )
        else:
            # If the scheduled time is in the future, push the task back into the scheduled queue
            await schedule_task(task_data, scheduled_at)
            print(f"Task {operation_id} is scheduled for {scheduled_at_dt}. Waiting...")
            return markdowns
    else:
        print(f"Unknown status: {status}")

    print("\nClosing crawler...")
    await crawler.close()

    return markdowns


async def process_urls(urls, max_concurrent, crawler, crawl_config, markdown_generator):
    success_count = 0
    fail_count = 0
    markdowns: list = []
    for i in range(0, len(urls), max_concurrent):
        batch = urls[i : i + max_concurrent]
        tasks = []

        for j, url in enumerate(batch):
            # Unique session_id per concurrent sub-task
            session_id = f"parallel_session_{i + j}"
            result = basic_crawl_operation(
                url, crawler, crawl_config, markdown_generator, session_id
            )
            tasks.append(result)

        # Gather results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Evaluate results
        for url, result in zip(batch, results):
            if isinstance(result, Exception):
                print(f"Error crawling {url}: {result}")
                fail_count += 1
                markdowns.append(False)
            elif result.success:
                print(f"Successfully crawled {url}")
                markdowns.append(True)
                success_count += 1
            else:
                fail_count += 1

    return markdowns
