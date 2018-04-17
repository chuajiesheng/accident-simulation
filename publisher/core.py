import random
import time
import json
from datetime import datetime
import configparser
import pika
import atexit


class AccidentRetriever:
    MQ_SECTION  = 'RabbitMQ'
    MQ_SERVER = 'server'
    MQ_USERNAME = 'username'
    MQ_PASSWORD = 'password'
    MQ_EXCHANGE = 'exchange'

    def __init__(self, boundary, interval):
        self.boundary = boundary
        self.interval = interval
        self.connection = self.setup_connection()
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name(), exchange_type='topic')
        atexit.register(self.close_connection, self)

    def setup_connection(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        if self.MQ_SECTION not in config.sections():
            raise MissingConfigurationError('Missing {} section'.format(self.MQ_SECTION))

        keys = [key for key in config[self.MQ_SECTION]]
        if self.MQ_SERVER not in keys:
            raise MissingConfigurationError('Missing {} key'.format(self.MQ_SERVER))

        if self.MQ_USERNAME not in keys:
            raise MissingConfigurationError('Missing {} key'.format(self.MQ_USERNAME))

        if self.MQ_PASSWORD not in keys:
            raise MissingConfigurationError('Missing {} key'.format(self.MQ_PASSWORD))

        server = config[self.MQ_SECTION][self.MQ_SERVER]
        username = config[self.MQ_SECTION][self.MQ_USERNAME]
        password = config[self.MQ_SECTION][self.MQ_PASSWORD]

        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=server, credentials=credentials))

        return connection

    def exchange_name(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        if self.MQ_SECTION not in config.sections():
            raise MissingConfigurationError('Missing {} section'.format(self.MQ_SECTION))

        if self.MQ_EXCHANGE not in config[self.MQ_SECTION].keys():
            raise MissingConfigurationError('Missing {} key'.format(self.MQ_EXCHANGE))

        return config[self.MQ_SECTION][self.MQ_EXCHANGE]

    def close_connection(self):
        self.connection.close()

    def watch(self):
        raise NotImplementedError

    @staticmethod
    def publish(payload):
        raise NotImplementedError


class RandomAccidentRetrieve(AccidentRetriever):
    def __init__(self, boundary, interval):
        self.r = random.Random(42)
        super(RandomAccidentRetrieve, self).__init__(boundary, interval)

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


class MissingConfigurationError(Exception):
    pass


class ServiceError(Exception):
    pass


class ServicePayloadError(Exception):
    pass
