import logging


def get_logger(name: str, debug: bool = False) -> logging.Logger:
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

    return logger
