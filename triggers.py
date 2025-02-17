import json
import time
from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlResult,
    DefaultMarkdownGenerator,
    PruningContentFilter,
)


from actions import basic_crawl  # , infinite_scroll
# from dynamic_selectors import auto_detect_selectors

from pydantic import BaseModel


# This class likely represents a product entity and inherits from a base model class.
class Product(BaseModel):
    name: str
    price: str


async def event_stream(url):
    """Generator for streaming events."""
    browser_config = BrowserConfig(
        headless=True,
        verbose=True,  # corrected from 'verbos=False'
        extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
        browser_type="chromium",
        viewport_height=600,
        viewport_width=800,  # Smaller viewport for better performance
    )  # Default browser configuration

    markdown_generator = DefaultMarkdownGenerator(
        content_filter=PruningContentFilter(),  # In case you need fit_markdown
    )

    try:
        # Simulate the OpenAI-style event stream response with JSON payloads
        event_id = "chatcmpl-AiR2aDQ3E8gXvD3QgpWnDfODGM9pn"
        system_fingerprint = "fp_0aa8d3e20b"
        model = "gpt-4o-mini-2024-07-18"

        # "https://www.scrapingcourse.com/infinite-scrolling"
        async with AsyncWebCrawler(config=browser_config) as crawler:
            result: CrawlResult = await basic_crawl(
                url=url,
                crawler=crawler,
                markdown_generator=markdown_generator,
                session_id="hn_session",
            )
            # result2 = await load_more(url=url, crawler=crawler, markdown_generator=markdown_generator,session_id="hn_session")
            # print("next products loaded. Count:", result.cleaned_html.count("<img"))
            # print(f"Num Images on Scrolling:", len(result.media["images"]))
            # print(f" items:",result.metadata, result.cleaned_html)
            # print(f"Page load more:", result.media["images"])
            fit_md = result.markdown  # Most relevant content in markdown
            print(fit_md)  # Print first 500 characters
            # Simulate chunking the markdown content into smaller parts
            chunk_size = 500  # Define the chunk size (number of characters)
            for i in range(0, len(fit_md), chunk_size):
                chunk = fit_md[i : i + chunk_size]
                # Generate the OpenAI-style JSON event structure
                message = {
                    "id": event_id,
                    "object": "chat.completion.chunk",
                    "created": int(
                        time.time()
                    ),  # Use the current time for the "created" field
                    "model": model,
                    "system_fingerprint": system_fingerprint,
                    "choices": [
                        {"index": 0, "delta": {"content": chunk}}
                    ],  # The chunk as content
                    "usage": None,
                }
                # Send the chunk as an event with data in JSON format
                yield f"data: {json.dumps(message)}\n\n"
            # After finishing all chunks, stream the "stop" event to indicate completion
            stop_message = {
                "id": event_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model,
                "system_fingerprint": system_fingerprint,
                "choices": [{}],  # Empty choice object to indicate stop
                "usage": None,
            }
            yield f"data: {json.dumps(stop_message)}\n\n"

            # Optionally, you can send some "done" information
            done_message = {
                "id": event_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model,
                "system_fingerprint": system_fingerprint,
                "choices": [],
                "usage": {
                    "prompt_tokens": 5702,
                    "completion_tokens": 1183,
                    "total_tokens": 6885,
                    "prompt_tokens_details": {"cached_tokens": 0},
                    "completion_tokens_details": {"reasoning_tokens": 0},
                },
            }
            yield f"data: {json.dumps(done_message)}\n\n"
            yield f"data: {'[DONE]'}\n\n"
        # while True:
        #     await asyncio.sleep(1)  # Simulate periodic events
        #     yield f"data: Hello, this is an event at {asyncio.get_event_loop().time()}\n\n"

    except Exception as e:
        # Log the exception and raise an HTTPException
        print(f"Error occurred: {e}")
        raise Exception(
            status_code=500, detail="Internal Server Error while streaming events."
        )


async def arrayBuffer_basic_crawl(url) -> bytes:
    try:
        bytes_buffer = await json_basic_crawl(url)  # Convert string to bytes buffer
        return bytes_buffer

    except Exception as e:
        # Log the exception and raise an HTTPException
        print(f"Error occurred: {e}")
        raise Exception(
            status_code=500,
            detail="Internal Server Error while returning array buffer.",
        )


async def json_basic_crawl(url):
    """Generate basic crawler."""

    browser_config = BrowserConfig(
        headless=True,
        verbose=True,  # corrected from 'verbos=False'
        extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
        browser_type="chromium",
        viewport_height=600,
        viewport_width=800,  # Smaller viewport for better performance
    )  # Default browser configuration

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

    try:
        # Simulate the OpenAI-style event stream response with JSON payloads
        event_id = "chatcmpl-AiR2aDQ3E8gXvD3QgpWnDfODGM9pn"
        # system_fingerprint = "fp_0aa8d3e20b"
        # model = "gpt-4o-mini-2024-07-18"

        # if not model:
        #     model = None

        # "https://www.scrapingcourse.com/infinite-scrolling"
        async with AsyncWebCrawler(config=browser_config) as crawler:
            result = await basic_crawl(
                url=url,
                crawler=crawler,
                markdown_generator=markdown_generator,
                session_id="hn_session",
            )

            # selectors = auto_detect_selectors(result.html)
            # print("CSS Selectors:", selectors['css_selectors'])

            fit_md = result.markdown  # Most relevant content in markdown format
            # Generate the OpenAI-style JSON event structure
            data = {
                "code": 200,
                "status": 20000,
                "data": {
                    "id": event_id,
                    "title": "",
                    "description": "",
                    "urls": url,
                    "created": int(
                        time.time()
                    ),  # Use the current time for the "created" field
                    "content": fit_md,  # The chunk as content
                    "metadata": result.metadata,
                    "media": {
                        "images": result.media["images"],
                        "videos": result.media["videos"],
                        "audios": result.media["audios"],
                    },
                    "usage": "",
                },
            }
            return json.dumps(data).encode("utf-8")

    except Exception as e:
        # Log the exception and raise an HTTPException
        print(f"Error occurred: {e}")
        raise Exception(
            status_code=500,
            detail="Internal Server Error while returning array buffer.",
        )


async def basic_crawl_operation(
    url, crawler: AsyncWebCrawler, crawl_config, markdown_generator, session_id
):
    """Generate basic crawler."""

    try:
        result = await crawler.arun(
            url=url,
            config=crawl_config,
            session_id=session_id,
            markdown_generator=markdown_generator,
        )

        # await basic_crawl(
        #     url=url,
        #     crawler=crawler,
        #     markdown_generator=markdown_generator,
        #     session_id="hn_session",
        #     # llm_strategy=None,
        # )

        if result.success:
            # 5. The extracted content is presumably JSON
            # data = json.loads(result.extracted_content)
            # print("Extracted items:", data)

            # selectors = auto_detect_selectors(result.html)
            # print("CSS Selectors:", selectors['css_selectors'])
            # return result  # Most relevant content in markdown format
            # 6. Show usage stats
            # llm_strategy.show_usage()  # prints token usage
            # return data
            print("Results:", "Yes")
        else:
            print("Error:", result.error_message)
            # raise Exception(result.error_message)

        return result
    except Exception as e:
        # Log the exception and raise an HTTPException
        print(f"Error occurred: {e}")
        raise Exception(
            status_code=500,
            detail="Internal Server Error while returning array buffer.",
        )
