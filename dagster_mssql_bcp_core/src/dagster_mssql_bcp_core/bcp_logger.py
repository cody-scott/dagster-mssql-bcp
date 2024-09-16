import re
from logging import INFO, Logger

from dagster import get_dagster_logger


class BCPLogger(Logger):
    dagster_logger = None
    cleaner_regex = r"(?<=-P).*(?=-f)|(?<=-U).*(?=-d)"

    def __init__(
        self, name, format="%(asctime)s | %(levelname)s | %(message)s", level=INFO
    ):
        self.dagster_logger = get_dagster_logger()

    def info(self, message, **kwargs):
        output_txt = re.sub(self.cleaner_regex, "*" * 5, message)
        if self.dagster_logger is not None:
            self.dagster_logger.debug(output_txt)

    def debug(self, message, **kwargs):
        output_txt = re.sub(self.cleaner_regex, "*" * 5, message)
        if self.dagster_logger is not None:
            self.dagster_logger.debug(output_txt)
