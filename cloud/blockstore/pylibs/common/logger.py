import argparse
import logging

from typing import NamedTuple


_DEFAULT_FORMAT = '[ %(name)-8s ] %(asctime)s [ %(levelname)-8s ] %(message)s'
_TEAMCITY_FORMAT = '##teamcity[message text=\'|[ %(name)-8s |] %(message)s\' status=\'NORMAL\' ]'


class _LoggingConfig(NamedTuple):
    logger_format: str
    log_level: int

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> '_LoggingConfig':
        log_level = logging.INFO
        if getattr(args, 'quite', False):
            log_level = logging.CRITICAL
        elif getattr(args, 'verbose', False):
            log_level = logging.DEBUG
        logger_format = _DEFAULT_FORMAT
        if getattr(args, 'teamcity', False):
            logger_format = _TEAMCITY_FORMAT
        return cls(logger_format=logger_format, log_level=log_level)


def create_logger(name: str, args: argparse.Namespace):
    """Creates logger instance with custom handlers.

    Use args.quite/args.verbose to set verbosity level.
    Use args.teamcity to set teamcity logging format.

    Attributes:
      name: logger name.
      args: instance returned by argparse parse_args() function.
    """

    logging_config = _LoggingConfig.from_args(args)
    logger = logging.getLogger(name)
    logger.setLevel(logging_config.log_level)
    handler = logging.StreamHandler()
    handler.setLevel(logging_config.log_level)
    formatter = logging.Formatter(logging_config.logger_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def setup_global_logger(args: argparse.Namespace):
    logging_config = _LoggingConfig.from_args(args)
    root = logging.root
    root.setLevel(logging_config.log_level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(logging_config.logger_format)
    handler.setFormatter(formatter)
    root.addHandler(handler)

    for _handler in root.handlers:
        _handler.setFormatter(formatter)
        _handler.setLevel(logging_config.log_level)
