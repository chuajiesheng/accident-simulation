import configparser
import multiprocessing, time, signal
import pika
import atexit
import signal

from publisher.core import MissingConfigurationError


class Master:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        self.deployment_manager = multiprocessing.Process(target=self.start_deployment,
                                                          args=(['malaysia.klang_valley'],))
        self.deployment_manager.start()

    def exit(self):
        if self.deployment_manager.is_alive():
            self.deployment_manager.terminate()

    @staticmethod
    def start_deployment(binding_keys):
        retriever = DeploymentMaster(binding_keys)
        retriever.consume()

    def start(self):
        while True:
            time.sleep(5)
            print('.' if self.deployment_manager.is_alive() else 'x', end='', flush=True)


class DeploymentMaster:
    MQ_SECTION  = 'RabbitMQ'
    MQ_SERVER = 'server'
    MQ_USERNAME = 'username'
    MQ_PASSWORD = 'password'
    MQ_EXCHANGE = 'exchange'

    def __init__(self, binding_keys):
        connection = self.setup_connection()

        self.connection = connection
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name(), exchange_type='topic')

        def close_connection():
            connection.close()

        atexit.register(close_connection)

        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        for binding_key in binding_keys:
            self.channel.queue_bind(exchange=self.exchange_name(), queue=queue_name, routing_key=binding_key)

        self.channel.basic_consume(self.process, queue=queue_name, no_ack=True)

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

    def consume(self):
        self.channel.start_consuming()

    def process(self, channel, method, properties, body):
        print(" [x] %r:%r" % (method.routing_key, body))


if __name__ == "__main__":
    m = Master()
    m.start()
