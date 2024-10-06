"""Create the format for logging"""

import logging
import sys


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)-20s %(message)s",
)


def get_logger(logger_name):
    """Takes the logger name and creates the logging format.
    Args:
        logger_name
    Returns:
        Object: a logging object
    """
    return logging.getLogger(logger_name)
