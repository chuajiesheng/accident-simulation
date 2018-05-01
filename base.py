import logging
import threading
import json
from datetime import datetime
from enum import Enum


class ServiceError(Exception):
    pass


class ServicePayloadError(Exception):
    pass


class MissingConfigurationError(Exception):
    pass


class StopConsuming(Exception):
    pass


def setup_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        return logger

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


class AccidentLocation:
    @staticmethod
    def from_dict(o):
        return AccidentLocation(o['lat'], o['long'])

    def __init__(self, lat, long):
        self.lat = lat
        self.long = long

    def to_dict(self):
        return {
            'lat': self.lat,
            'long': self.long
        }


class Boundary:
    @staticmethod
    def from_dict(o):
        return Boundary(o['left'], o['top'], o['right'], o['bottom'])

    def __init__(self, left, top, right, bottom):
        self.left = left
        self.top = top
        self.right = right
        self.bottom = bottom

    def to_dict(self):
        return {
            'left': self.left,
            'top': self.top,
            'right': self.right,
            'bottom': self.bottom
        }


class AccidentPayload:
    @staticmethod
    def from_dict(o):
        boundary = Boundary.from_dict(o['boundary'])
        accident = AccidentLocation.from_dict(o['accident'])
        p = AccidentPayload(boundary, accident)
        p.utc_timestamp = o['utc_timestamp']

        return p

    def __init__(self, boundary, location, utc_timestamp=datetime.utcnow().timestamp()):
        self.boundary = boundary
        self.location = location
        self.utc_timestamp = utc_timestamp

    def to_dict(self):
        return {
            'utc_timestamp': self.utc_timestamp,
            'boundary': self.boundary.to_dict(),
            'accident': self.location.to_dict()
        }


class PlayerInstruction(Enum):
    WHERE = 'where'
    GO = 'go'


class AccidentDeployment:
    @staticmethod
    def from_dict(o):
        action = PlayerInstruction(o['action'])
        payload = AccidentPayload.from_dict(o['payload'])
        d = AccidentDeployment(action, payload)
        d.utc_decision_time = o['utc_decision_time']

        return d

    def __init__(self, action, payload):
        self.action = action
        self.payload = payload
        self.utc_decision_time = datetime.utcnow().timestamp()

    def to_dict(self):
        return {
            'action': self.action.value,
            'payload': self.payload.to_dict(),
            'utc_decision_time': self.utc_decision_time,
        }