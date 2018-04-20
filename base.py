import logging


class ServiceError(Exception):
    pass


class ServicePayloadError(Exception):
    pass


class MissingConfigurationError(Exception):
    pass


def setup_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger
