"""Logger utils."""

import logging

from prefect import get_run_logger
from prefect.exceptions import MissingContextError


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the flows, relying on to a fallback if not available.

    Args:
        name (str): The name of the logger.
    """
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(name)
