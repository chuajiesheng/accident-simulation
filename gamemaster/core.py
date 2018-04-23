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
    players = {}

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

        response = 'ok'
        team_uuid = payload['team_uuid']
        self.logger.debug('spawning team=%r, size=%s', team_uuid, payload['player_count'])
        for i in range(payload['player_count']):
            if team_uuid not in self.players.keys():
                self.players[team_uuid] = []

            player = Player(team_uuid, i)
            player.start()
            self.players[team_uuid].append(player)

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
            def terminate_group(players):
                self.logger.debug('terminating pids=%r', list(map(lambda player: player.pid, players)))
                self.logger.debug('terminating is_alive=%r', list(map(lambda player: player.is_alive(), players)))
                deque(map(lambda player: player.terminate(), players), maxlen=0)

            for k in self.players.keys():
                terminate_group(self.players[k])

        self.logger.debug('serve() completed')


class Player(Process):
    EXCHANGE_NAME = 'player'

    def __init__(self, group_uuid, player_id):
        super(Player, self).__init__(target=self.consume, daemon=True)
        self.group_uuid = group_uuid
        self.player_id = player_id
        self.queue_name = '{}.{}'.format(group_uuid, player_id)
        self.logger = setup_logging('Player {}'.format(self.queue_name))
        self.logger.debug('initiated')

    def handle_rpc_call(self, channel, method, props, body):
        self.logger.debug('method.routing_key=%s; body=%s;', method.routing_key, body)
        response = 'ok'
        channel.basic_publish(exchange='',
                              routing_key=props.reply_to,
                              properties=pika.BasicProperties(correlation_id=props.correlation_id),
                              body=str(response))
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def consume(self):
        connection = RabbitMQ.setup_connection()

        def close_connection():
            self.logger.debug('Closing connection')
            connection.close()

        atexit.register(close_connection)

        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self.handle_rpc_call, queue=self.queue_name)

        self.logger.debug('binding to RabbitMQ with keys=%r', self.queue_name)

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


if __name__ == "__main__":
    retriever = GameMaster()
    retriever.serve()
