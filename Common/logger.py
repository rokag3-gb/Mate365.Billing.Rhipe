import logging
import os

from Common.logger_tool import get_default_logger

log_level = logging.INFO if os.getenv('CRAWLER_ENV') == 'prod' else logging.DEBUG
LOGGER = get_default_logger(__name__, level=log_level)