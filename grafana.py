from prometheus_client import Counter, Gauge, Histogram

# Define metrics
QUEUE_SIZE = Gauge("scraper_queue_size", "Current operation queue size")
CPU_USAGE = Gauge("scraper_cpu_usage", "CPU usage percentage")
MEMORY_USAGE = Gauge("scraper_memory_usage", "Memory usage percentage")
OPERATION_DURATION = Histogram(
    "scraper_operation_duration_seconds", "Time spent on scraping operations"
)
ERROR_COUNTER = Counter("scraper_errors_total", "Total number of scraping errors")
