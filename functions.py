import urllib.parse
import re


def url_to_unique_name(url):
    parsed_url = urllib.parse.urlparse(url)
    slug = re.sub(r"[^a-zA-Z0-9_\-]", "-", parsed_url.path)
    slug = slug.strip("-")
    return f"{parsed_url.netloc}_{slug}"


def crawl_operation_status_color(status: str) -> str:
    """
    Returns the color associated with the given crawl operation status.

    Args:
        status (str): The crawl operation status.

    Returns:
        str: The color associated with the status.
    """
    color_map = {
        'IN_PROGRESS': 'cyan',
        'FAILED': 'red',
        'CANCELED': 'deep-orange',
        'SCHEDULED': 'emerald',
        'COMPLETED': 'green',
        'STARTED': 'blue',
        'READY': 'gray'
    }
    return color_map.get(status, 'gray')