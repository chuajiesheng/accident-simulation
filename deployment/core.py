import threading
import time
import queue

from mq import RabbitMQ
from core import setup_logging


class Master:
    def __init__(self):
        self.logger = setup_logging('Master')
        self.message_queue = queue.Queue()
        self.deployment_manager = DeploymentMaster(self.message_queue)
        self.logger.debug('initiated')

    def start(self):
        self.logger.debug('starting')
        self.deployment_manager.start()

        try:
            while True:
                time.sleep(5)
                self.logger.debug('deployment manager status=%s', self.deployment_manager.is_alive())
                self.logger.debug('empty_queue=%s', self.message_queue.empty())
                try:
                    msg = self.message_queue.get(block=False)
                    self.logger.debug("message=%r", msg)
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
        self.logger = setup_logging('DeploymentMaster')

    def consume(self):
        connection = RabbitMQ.setup_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=RabbitMQ.accident_exchange_name(), exchange_type='topic')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        binding_keys = ['malaysia.klang_valley']
        for binding_key in binding_keys:
            channel.queue_bind(exchange=RabbitMQ.accident_exchange_name(), queue=queue_name, routing_key=binding_key)

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
