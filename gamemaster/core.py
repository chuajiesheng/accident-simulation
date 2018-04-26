from queue import Queue

import pika
import atexit
import json
from multiprocessing import Process
from threading import Thread
from collections import deque
import sys
import threading
import time

from deployment.core import Action
from mq import RabbitMQ
from base import setup_logging, deserialize_message, AccidentLocation


class GameMaster:
    players = {}

    def __init__(self):
        self.logger = setup_logging('GameMaster')

        connection = RabbitMQ.setup_connection()

        def close_connection():
            self.logger.debug('Closing connection')
            connection.close()

        atexit.register(close_connection)

        channel = connection.channel()
        channel.queue_declare(queue=RabbitMQ.game_master_queue_name())

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
        self.channel.basic_consume(self.on_request, queue=RabbitMQ.game_master_queue_name())

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
        self.object_name = '{}.player{}'.format(group_uuid, player_id)
        self.queue_name = self.object_name

        self.logger = setup_logging(self.object_name)

        self.state = PlayerState(self.object_name)

        self.logger.debug('initiated')

    def handle(self, message):
        self.logger.debug('handling message=%r', message)

        action = Action(message['action'])
        response = {'state': 'ok'}

        if action == Action.GO:
            payload = message['payload']
            self.state.move_to(payload['accident']['lat'], payload['accident']['long'])
        elif action == Action.WHERE:
            response['player'] = {
                'lat': self.state.lat,
                'long': self.state.long
            }

        return response

    def handle_rpc_call(self, channel, method, props, body):
        self.logger.debug('method.routing_key=%s; body=%s;', method.routing_key, body)
        response = self.handle(deserialize_message(body))
        channel.basic_publish(exchange='',
                              routing_key=props.reply_to,
                              properties=pika.BasicProperties(correlation_id=props.correlation_id),
                              body=json.dumps(response))
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def update(self):
        while True:
            self.logger.debug('update')
            time.sleep(3)

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

        update_thread = Thread(target=self.update, name='{}.update_thread'.format(self.object_name), daemon=True)

        try:
            self.logger.debug('start update thread')
            update_thread.start()
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


class PlayerState:
    condition = threading.Condition()

    def __init__(self, player_name):
        self.player_name = player_name
        self.logger = setup_logging('{}.state'.format(player_name))

        self.lat = 0
        self.long = 0
        self.queue = Queue.queue()

        self.logger.debug('initiated')

    def move_to(self, lat, long):
        self.condition.acquire()

        self.lat = lat
        self.long = long

        self.logger.debug('moving to lat=%r, long=%r', lat, long)

        self.condition.notify_all()
        self.condition.release()

    def queue_destination(self, accident):
        if not type(accident) == AccidentLocation:
            raise ValueError('not a AccidentLocation object')

        self.queue.put(accident)


if __name__ == "__main__":
    retriever = GameMaster()
    retriever.serve()
