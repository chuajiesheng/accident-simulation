import random
import time
from datetime import datetime
import atexit

from mq import RabbitMQ


class AccidentRetriever:
    def __init__(self, boundary, interval):
        self.boundary = boundary
        self.interval = interval

        connection = RabbitMQ.setup_connection()

        self.connection = connection
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=RabbitMQ.exchange_name(), exchange_type='topic')

        def close_connection():
            connection.close()

        atexit.register(close_connection)

    def watch(self):
        raise NotImplementedError

    @staticmethod
    def publish(payload):
        raise NotImplementedError


class RandomAccidentRetriever(AccidentRetriever):
    def __init__(self, boundary, interval):
        self.r = random.Random(42)
        super(RandomAccidentRetriever, self).__init__(boundary, interval)

    def watch(self):
        while True:
            lat = self.boundary.bottom + self.r.random() * (self.boundary.top - self.boundary.bottom)
            long = self.boundary.left + self.r.random() * (self.boundary.right - self.boundary.left)

            self.publish(AccidentPayload(self.boundary, AccidentLocation(lat, long)))
            time.sleep(self.interval)

    @staticmethod
    def publish(payload):
        raise NotImplementedError


class Boundary:
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


class AccidentLocation:
    def __init__(self, lat, long):
        self.lat = lat
        self.long = long

    def to_dict(self):
        return {
            'lat': self.lat,
            'long': self.long
        }


class AccidentPayload:
    def __init__(self, boundary, location):
        self.boundary = boundary
        self.location = location

    def to_dict(self):
        return {
            'utc_timestamp': datetime.utcnow().timestamp(),
            'boundary': self.boundary.to_dict(),
            'accident': self.location.to_dict()
        }
