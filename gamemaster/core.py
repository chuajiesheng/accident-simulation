import pika
import atexit
import json

from mq import RabbitMQ
from base import setup_logging


class GameMaster:
    QUEUE_NAME = 'game_master'

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

        channel.basic_publish(exchange='',
                              routing_key=props.reply_to,
                              properties=pika.BasicProperties(correlation_id=props.correlation_id),
                              body=str(response))

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def serve(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.on_request, queue=self.QUEUE_NAME)

        self.logger.debug('ready for rpc request')
        self.channel.start_consuming()


if __name__ == "__main__":
    retriever = GameMaster()
    retriever.serve()
