import logging
import os

from teams_logger import TeamsHandler


def get_default_logger(logger_name: str, level: logging = logging.DEBUG):
    '''

    :param logger_name:
    :param slack_channel:
    :param level:
    :return: logging.GetLogger()
    '''
    formatter = logging.Formatter(
        '%(asctime)s - %(filename)s : %(lineno)d line - %(funcName)s - %(levelname)s - %(message)s')

    # Teams Handler
    teams_handler = TeamsHandler(url=os.environ['TEAMS_WEBHOOK_LOG_URL'], level=logging.WARNING)
    teams_handler.setFormatter(formatter)

    log_stream_handler = logging.StreamHandler()
    log_stream_handler.setFormatter(formatter)
    # Create logger
    logger = logging.getLogger(logger_name)

    logger.addHandler(teams_handler)
    logger.addHandler(log_stream_handler)
    logger.setLevel(level)
    return logger











































