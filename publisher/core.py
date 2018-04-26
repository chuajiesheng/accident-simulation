import random
import time
from datetime import datetime
import atexit

from mq import RabbitMQ
from base import setup_logging, AccidentLocation


class AccidentRetriever:
    def __init__(self, boundary, interval):
        self.logger = setup_logging('AccidentRetriever')

        self.boundary = boundary
        self.interval = interval

        connection = RabbitMQ.setup_connection()

        self.connection = connection
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=RabbitMQ.accident_exchange_name(), exchange_type='topic')

        def close_connection():
            self.logger.debug('Closing connection')
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
        self.logger = setup_logging('RandomAccidentRetriever')

    def watch(self):
        try:
            while True:
                lat = self.boundary.bottom + self.r.random() * (self.boundary.top - self.boundary.bottom)
                long = self.boundary.left + self.r.random() * (self.boundary.right - self.boundary.left)

                self.publish(AccidentPayload(self.boundary, AccidentLocation(lat, long)))
                self.logger.debug('Sleep secs=%s', self.interval)
                time.sleep(self.interval)
        except KeyboardInterrupt:
            self.logger.debug('KeyboardInterrupt')

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
