from datetime import datetime
import os
from pathlib import Path
import re
import yaml
from typing import Dict


def is_scheduled_time_in_past(scheduled_at: int) -> tuple[bool, datetime]:
    """
    Checks if the given scheduled time (in milliseconds since the epoch) is in the past relative to the current system time.
    Args:
        scheduled_at (int): The scheduled time in milliseconds since the Unix epoch.
    Returns:
        bool: True if the scheduled time is in the past or equal to the current time, False otherwise.
    """
    
    scheduled_at_dt = datetime.fromtimestamp(scheduled_at / 1000)
    now = datetime.now()
    
    return now >= scheduled_at_dt, scheduled_at_dt


def load_config() -> Dict:
    """
    Load and return application configuration with environment variable expansion.

    This function reads the 'config.yml' file located in the same directory as this script,
    expands any environment variable placeholders in the form of ${VAR} by tagging them with
    '!env_var', and parses the YAML content into a Python dictionary.

    Returns:
        Dict: The parsed configuration as a dictionary. Returns an empty dictionary if
        the file cannot be read or parsed.

    Raises:
        None: All exceptions are caught and result in an empty dictionary being returned.
    """

    config_path = Path(__file__).parent / "config.yml"
    try:
        with open(config_path, "r", encoding='utf-8') as config_file:
            content = config_file.read()

        def env_var_constructor(loader, node):
            value = loader.construct_scalar(node)
            return re.sub(r'\$\{([^}^{]+)\}', lambda m: os.environ.get(m.group(1), ""), value)

        class EnvVarLoader(yaml.SafeLoader):
            pass

        EnvVarLoader.add_implicit_resolver('!env_var', re.compile(r'.*\$\{[^}^{]+\}.*'), None)
        EnvVarLoader.add_constructor('!env_var', env_var_constructor)
        
        return yaml.load(content, Loader=EnvVarLoader) or {}
    except Exception:
        return {}