import pika
import atexit
import json
from multiprocessing import Process
from collections import deque
import sys

from mq import RabbitMQ
from base import setup_logging


class GameMaster:
    QUEUE_NAME = 'game_master'
    assessors = {}

    def __init__(self):
        self.logger = setup_logging('GameMaster')

        connection = RabbitMQ.setup_connection()

        def close_connection():
            self.logger.debug('Closing connection')
            connection.close()

        atexit.register(close_connection)

        channel = connection.channel()
        channel.queue_declare(queue=self.QUEUE_NAME)

        self.connection = connection
        self.channel = channel

    def on_request(self, channel, method, props, body):
        payload = json.loads(body)
        self.logger.debug('payload=%r', payload)

        response = 'ok'
        group_uuid = payload['group_uuid']
        for i in range(payload['assessor_count']):
            if group_uuid not in self.assessors.keys():
                self.assessors[group_uuid] = []

            assessor = Assessor(group_uuid, i)
            assessor.start()
            self.assessors[group_uuid].append(assessor)

        channel.basic_publish(exchange='',
                              routing_key=props.reply_to,
                              properties=pika.BasicProperties(correlation_id=props.correlation_id),
                              body=str(response))

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def serve(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.on_request, queue=self.QUEUE_NAME)

        self.logger.debug('ready for rpc request')

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.debug('KeyboardInterrupt')
        finally:
            def terminate_group(assessors):
                self.logger.debug('terminating pids=%r', list(map(lambda assessor: assessor.pid, assessors)))
                self.logger.debug('terminating is_alive=%r', list(map(lambda assessor: assessor.is_alive(), assessors)))
                deque(map(lambda assessor: assessor.terminate(), assessors), maxlen=0)

            for k in self.assessors.keys():
                terminate_group(self.assessors[k])

        self.logger.debug('serve() completed')


class Assessor(Process):
    EXCHANGE_NAME = 'assessor'

    def __init__(self, group_uuid, assessor_id):
        super(Assessor, self).__init__(target=self.consume, daemon=True)
        self.group_uuid = group_uuid
        self.assessor_id = assessor_id
        self.logger = setup_logging('Assessor {}.{}'.format(group_uuid, assessor_id))
        self.logger.debug('initiated')

    def consume(self):
        connection = RabbitMQ.setup_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=self.EXCHANGE_NAME, exchange_type='topic')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        binding_key = '{}.{}'.format(self.group_uuid, self.assessor_id)
        channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=queue_name, routing_key=binding_key)
        self.logger.debug('binding to RabbitMQ with keys=%r', binding_key)

        channel.basic_consume(self.process, queue=queue_name, no_ack=True)

        try:
            self.logger.debug('start consuming')
            channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.debug('KeyboardInterrupt')
        except:
            self.logger.debug("unexpected error=%s", sys.exc_info()[0])
        finally:
            channel.stop_consuming()
            connection.close()
            self.logger.debug('stopping')

    def process(self, channel, method, properties, body):
        self.logger.debug('method.routing_key=%s; body=%s;', method.routing_key, body)


if __name__ == "__main__":
    retriever = GameMaster()
    retriever.serve()
