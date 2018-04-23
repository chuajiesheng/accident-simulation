import logging
import threading
import json


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


def deserialize_message(body):
    s = body.decode('utf-8')
    assert type(s) is str
    d = json.loads(s)
    assert type(d) is dict

    return d


class StoppableThread(threading.Thread):
    def __init__(self):
        super(StoppableThread, self).__init__(daemon=True, target=self.consume)
        self._stop_event = threading.Event()

    def consume(self):
        raise NotImplemented

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()