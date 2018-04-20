import configparser
import pika

from core import MissingConfigurationError


class RabbitMQ:
    MQ_SECTION = 'RabbitMQ'
    MQ_SERVER = 'server'
    MQ_USERNAME = 'username'
    MQ_PASSWORD = 'password'
    MQ_EXCHANGE = 'exchange'

    @staticmethod
    def setup_connection():
        config = configparser.ConfigParser()
        config.read('config.ini')

        if RabbitMQ.MQ_SECTION not in config.sections():
            raise MissingConfigurationError('Missing {} section'.format(RabbitMQ.MQ_SECTION))

        keys = [key for key in config[RabbitMQ.MQ_SECTION]]
        if RabbitMQ.MQ_SERVER not in keys:
            raise MissingConfigurationError('Missing {} key'.format(RabbitMQ.MQ_SERVER))

        if RabbitMQ.MQ_USERNAME not in keys:
            raise MissingConfigurationError('Missing {} key'.format(RabbitMQ.MQ_USERNAME))

        if RabbitMQ.MQ_PASSWORD not in keys:
            raise MissingConfigurationError('Missing {} key'.format(RabbitMQ.MQ_PASSWORD))

        server = config[RabbitMQ.MQ_SECTION][RabbitMQ.MQ_SERVER]
        username = config[RabbitMQ.MQ_SECTION][RabbitMQ.MQ_USERNAME]
        password = config[RabbitMQ.MQ_SECTION][RabbitMQ.MQ_PASSWORD]

        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=server, credentials=credentials))
        return connection

    @staticmethod
    def exchange_name():
        config = configparser.ConfigParser()
        config.read('config.ini')

        if RabbitMQ.MQ_SECTION not in config.sections():
            raise MissingConfigurationError('Missing {} section'.format(RabbitMQ.MQ_SECTION))

        if RabbitMQ.MQ_EXCHANGE not in config[RabbitMQ.MQ_SECTION].keys():
            raise MissingConfigurationError('Missing {} key'.format(RabbitMQ.MQ_EXCHANGE))

        return config[RabbitMQ.MQ_SECTION][RabbitMQ.MQ_EXCHANGE]
