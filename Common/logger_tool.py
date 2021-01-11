import logging
import os

from slacker_log_handler import SlackerLogHandler

from rhipe_crawler_src.envlist import slack_api_token


def get_default_logger(logger_name: str, slack_channel: str = "#dev", level: logging = logging.DEBUG):
    '''

    :param logger_name:
    :param slack_channel:
    :param level:
    :return: logging.GetLogger()
    '''
    formatter = logging.Formatter(
        '%(asctime)s - %(filename)s : %(lineno)d line - %(funcName)s - %(levelname)s - %(message)s')

    log_slack_handler = SlackerLogHandler(slack_api_token, slack_channel,
                                          stack_trace=True)
    log_slack_handler.setLevel(logging.WARNING)
    log_slack_handler.setFormatter(formatter)

    log_stream_handler = logging.StreamHandler()
    log_stream_handler.setFormatter(formatter)
    # Create logger
    logger = logging.getLogger(logger_name)

    logger.addHandler(log_slack_handler)
    logger.addHandler(log_stream_handler)
    logger.setLevel(level)
    return logger











































