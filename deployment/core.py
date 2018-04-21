import time
import queue
import uuid
import pika
import json
import functools

from mq import RabbitMQ
from base import setup_logging, StoppableThread
from gamemaster import core


class Master:
    assessor_group = None

    def __init__(self):
        self.logger = setup_logging('Master')
        self.message_queue = queue.Queue()
        self.deployment_manager = DeploymentMaster(self.message_queue)
        self.logger.debug('initiated')

    def request_assessor_group_rpc(self, payload):
        connection = RabbitMQ.setup_connection()
        channel = connection.channel()

        result = channel.queue_declare(exclusive=True)
        callback_queue = result.method.queue

        corr_id = str(uuid.uuid4())

        def on_response(parent, correlation_id, ch, method, props, body):
            if correlation_id== props.correlation_id:
                parent.assessor_group = body
                parent.logger.debug('on_response, response=%r', body)

        channel.basic_consume(functools.partial(on_response, self, corr_id), no_ack=True, queue=callback_queue)
        channel.basic_publish(exchange='',
                              routing_key=core.GameMaster.QUEUE_NAME,
                              properties=pika.BasicProperties(reply_to=callback_queue,
                                                              correlation_id=corr_id),
                              body=json.dumps(payload))

        self.logger.debug('waiting to process event')
        while self.assessor_group is None:
            connection.process_data_events()

        self.logger.debug('response=%r', self.assessor_group)
        return self.assessor_group

    def start(self):
        self.logger.debug('request accessor group')
        self.request_assessor_group_rpc({
            'group_uuid': str(uuid.uuid4()),
            'assessors': 5
        })

        self.logger.debug('starting deployment manager')
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
            self.logger.debug('KeyboardInterrupt')
        finally:
            self.logger.debug('stopping deployment manager')
            self.deployment_manager.stop()
            self.deployment_manager.join()


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
