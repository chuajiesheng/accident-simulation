import configparser
import threading, time
import pika
import queue
import logging


from publisher.core import MissingConfigurationError


class Master:
    MQ_SECTION = 'RabbitMQ'
    MQ_SERVER = 'server'
    MQ_USERNAME = 'username'
    MQ_PASSWORD = 'password'
    MQ_EXCHANGE = 'exchange'

    def __init__(self):
        self.logger = self.setup_logging()
        self.message_queue = queue.Queue()
        self.deployment_manager = DeploymentMaster(self.message_queue)
        self.logger.debug('initiated')

    @staticmethod
    def setup_logging(name='Master'):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        logger.addHandler(ch)

        return logger

    def start(self):
        self.logger.debug('starting')
        self.deployment_manager.start()

        try:
            while True:
                time.sleep(5)
                self.logger.debug('deployment manager status={}'.format(self.deployment_manager.is_alive()))
                self.logger.debug('empty_queue={}'.format(self.message_queue.empty()))
                try:
                    msg = self.message_queue.get(block=False)
                    self.logger.debug("message='{}'".format(msg))
                except queue.Empty:
                    continue

        except KeyboardInterrupt:
            self.deployment_manager.stop()
            self.logger.debug('KeyboardInterrupt')
            self.deployment_manager.join()


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


class DeploymentMaster(StoppableThread):
    def __init__(self, message_queue):
        super(DeploymentMaster, self).__init__()
        self.message_queue = message_queue
        self.logger = Master.setup_logging('DeploymentMaster')

    @staticmethod
    def setup_connection():
        config = configparser.ConfigParser()
        config.read('config.ini')

        if Master.MQ_SECTION not in config.sections():
            raise MissingConfigurationError('Missing {} section'.format(Master.MQ_SECTION))

        keys = [key for key in config[Master.MQ_SECTION]]
        if Master.MQ_SERVER not in keys:
            raise MissingConfigurationError('Missing {} key'.format(Master.MQ_SERVER))

        if Master.MQ_USERNAME not in keys:
            raise MissingConfigurationError('Missing {} key'.format(Master.MQ_USERNAME))

        if Master.MQ_PASSWORD not in keys:
            raise MissingConfigurationError('Missing {} key'.format(Master.MQ_PASSWORD))

        server = config[Master.MQ_SECTION][Master.MQ_SERVER]
        username = config[Master.MQ_SECTION][Master.MQ_USERNAME]
        password = config[Master.MQ_SECTION][Master.MQ_PASSWORD]

        credentials = pika.PlainCredentials(username, password)
        return pika.BlockingConnection(pika.ConnectionParameters(host=server, credentials=credentials))

    @staticmethod
    def exchange_name():
        config = configparser.ConfigParser()
        config.read('config.ini')

        if Master.MQ_SECTION not in config.sections():
            raise MissingConfigurationError('Missing {} section'.format(Master.MQ_SECTION))

        if Master.MQ_EXCHANGE not in config[Master.MQ_SECTION].keys():
            raise MissingConfigurationError('Missing {} key'.format(Master.MQ_EXCHANGE))

        return config[Master.MQ_SECTION][Master.MQ_EXCHANGE]

    def consume(self):
        connection = self.setup_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange_name(), exchange_type='topic')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        binding_keys = ['malaysia.klang_valley']
        for binding_key in binding_keys:
            channel.queue_bind(exchange=self.exchange_name(), queue=queue_name, routing_key=binding_key)

        self.logger.debug('binding to RabbitMQ')

        channel.basic_consume(self.process, queue=queue_name, no_ack=True)
        channel.start_consuming()

    def process(self, channel, method, properties, body):
        self.logger.debug('method.routing_key=%s; body=%s;', method.routing_key, body)
        self.message_queue.put(body)

        if self.stopped():
            channel.stop_consuming()
            self.logger.debug('stop consuming')


if __name__ == "__main__":
    m = Master()
    m.start()
