# for async use
import os

# from redis.asyncio import Redis


from upstash_redis.asyncio import Redis


# pool = ConnectionPool(
#     host=os.getenv("REDIS_REST_URL"),
#     password=os.getenv("REDIS_REST_PASS"),
#     username="default",
#     decode_responses=True,
#     port=17141,
#     db=0,
#     max_connections=5,  # Limit connections to save RAM
# )

# redis = Redis(
#     host=os.getenv("REDIS_REST_URL"),
#     password=os.getenv("REDIS_REST_PASS"),
#     username="default",
#     decode_responses=True,
#     port=17141,
#     db=0,
# )

# if you want to automatically load the credentials from the environment
# redis = Redis.from_env()

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN"),
)
