import logging

__restful_functions_loggers = {}


def get_logger(name: str, debug: bool = False) -> logging.Logger:
    global __restful_functions_loggers

    if __restful_functions_loggers.get(name):
        return __restful_functions_loggers.get(name)

    logger = logging.getLogger(name)
    handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='[%Y-%m-%d %H:%M:%S %z]',
        )
    handler.setFormatter(formatter)

    if debug:
        handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        logger.setLevel(logging.INFO)

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    __restful_functions_loggers[name] = logger

    return logger
