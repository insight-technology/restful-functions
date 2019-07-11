import logging
from typing import Callable, Dict

__restful_functions_loggers: Dict[str, logging.Logger] = {}

__default_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='[%Y-%m-%d %H:%M:%S %z]')


def get_logger(
        name: str,
        *,
        logger_factory: Callable = logging.getLogger,
        formatter: logging.Formatter = __default_formatter,
        debug: bool = False) -> logging.Logger:
    global __restful_functions_loggers

    if name in __restful_functions_loggers:
        return __restful_functions_loggers[name]

    logger = logging.getLogger(name)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    if debug:
        logger.setLevel(logging.DEBUG)
        handler.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        handler.setLevel(logging.INFO)

    logger.addHandler(handler)

    __restful_functions_loggers[name] = logger

    return logger
