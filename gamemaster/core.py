import pika
import atexit
import json

from mq import RabbitMQ
from base import setup_logging, StoppableThread


class GameMaster:
    QUEUE_NAME = 'game_master'
    assessor_managers = {}

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
        self.assessor_managers[group_uuid] = accessor_manager = AccessorManager(group_uuid)
        accessor_manager.start()

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
            for k in self.assessor_managers:
                self.logger.debug('stopping %s=%r', 'AccessManager', k)
                assessor_manager = self.assessor_managers[k]
                assessor_manager.stop()
                assessor_manager.join(5)

                if assessor_manager.is_alive():
                    assessor_manager.stop_consuming()

        self.logger.debug('serve() completed')


class AccessorManager(StoppableThread):
    EXCHANGE_NAME = 'manager'

    def __init__(self, group_uuid):
        super(AccessorManager, self).__init__()
        self.group_uuid = group_uuid
        self.logger = setup_logging('AccessManager ({})'.format(group_uuid))
        self.stop_consuming = lambda: self.logger.debug('stop_consuming not implemented')

    def consume(self):
        connection = RabbitMQ.setup_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=self.EXCHANGE_NAME, exchange_type='topic')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        binding_keys = ['{}.#'.format(self.group_uuid)]
        for binding_key in binding_keys:
            channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=queue_name, routing_key=binding_key)

        self.logger.debug('binding to RabbitMQ with keys=%r', binding_keys)

        channel.basic_consume(self.process, queue=queue_name, no_ack=True)
        self.stop_consuming = lambda: channel.stop_consuming()

        channel.start_consuming()

    def process(self, channel, method, properties, body):
        self.logger.debug('method.routing_key=%s; body=%s;', method.routing_key, body)
        self.message_queue.put(body)

        if self.stopped():
            channel.stop_consuming()
            self.logger.debug('stop consuming')


if __name__ == "__main__":
    retriever = GameMaster()
    retriever.serve()
