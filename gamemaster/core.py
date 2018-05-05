import signal
import traceback
from datetime import datetime
from enum import Enum
import queue
import random

import pika
import atexit
import json
from multiprocessing import Process
from threading import Thread
from collections import deque
import sys
import threading
import time

from gamemaster.vehicle import driving_by_car
from mq import RabbitMQ
from base import setup_logging, deserialize_message, Boundary, AccidentDeployment, PlayerInstruction, StopConsuming

UPDATE_INTERVAL_IN_SECS = 3


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
        def create_team(uuid, count, boundary):
            self.logger.debug('spawning team=%r, size=%s within_boundary=%r',
                              uuid, count, boundary.to_dict())
            for i in range(count):
                if uuid not in self.players.keys():
                    self.players[uuid] = []

                player = Player(uuid, i, boundary)
                player.start()
                self.players[uuid].append(player)

            return 'created'

        def terminate_team(uuid):
            self.logger.debug('terminating team=%r', uuid)
            players = self.players.get(uuid) or []

            def terminate(player):
                self.logger.debug('terminating process=%r', player.pid)
                player.terminate()
                self.logger.debug('waiting process=%r', player.pid)
                player.join()
                return player.is_alive()

            if all(map(lambda p: not terminate(p), players)):
                self.logger.debug('remove team=%r from players', uuid)
                self.players.pop(uuid, None)

            return 'completed'

        payload = json.loads(body)
        request_type = payload['type']

        if request_type == 'request':
            team_uuid = payload['team_uuid']
            player_count = payload['player_count']
            team_boundary = Boundary.from_dict(payload['team_boundary'])
            response = create_team(team_uuid, player_count, team_boundary)
        elif request_type == 'terminate':
            response = terminate_team(payload['team_uuid'])
        else:
            response = 'ok'

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
            for k in self.players.keys():
                processes = self.players[k]
                self.logger.debug('waiting for pids=%r', list(map(lambda player: player.pid, processes)))
                deque(map(lambda player: player.join(), processes), maxlen=0)

        self.logger.debug('serve() completed')


class Player(Process):
    EXCHANGE_NAME = 'player'

    def __init__(self, group_uuid, player_id, boundary):
        super(Player, self).__init__(target=self.consume, daemon=True)
        self.group_uuid = group_uuid
        self.player_id = player_id
        self.boundary = boundary
        self.object_name = '{}.player{}'.format(group_uuid, player_id)
        self.queue_name = self.object_name

        self.logger = setup_logging(self.object_name)

        self.state = PlayerState(self.object_name, boundary)
        self.job_queue = queue.Queue()

        self.logger.debug('initiated')

    def handle(self, message):
        self.logger.debug('handling message=%r', message)

        action = PlayerInstruction(message['action'])
        response = {
            'state': 'ok',
            'player_id': self.player_id
        }

        if action == PlayerInstruction.GO:
            self.queue_destination(AccidentDeployment.from_dict(message))
        elif action == PlayerInstruction.STATUS:
            self.logger.debug('checking status of player')

            current_location = {'lat': self.state.lat, 'long': self.state.long}
            response['status'] = self.state.status.value
            response['current_location'] = current_location

            is_en_route = self.state.status == Status.EN_ROUTE
            if not is_en_route:
                response['destination'] = current_location
            else:
                response['destination'] = {
                    'lat': self.state.plan[-1]['lat']['end'],
                    'long': self.state.plan[-1]['long']['end']
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
            self.logger.debug('heartbeat')

            if self.state.is_idle() and not self.job_queue.empty():
                job = self.job_queue.get()
                self.logger.debug('job=%r', job.to_dict())

                source = self.state.current_location()
                destination = job.payload.location.to_dict()
                plan = driving_by_car(source, destination)
                self.state.travel_to(plan)

            self.state.update()
            time.sleep(UPDATE_INTERVAL_IN_SECS)

    def queue_destination(self, deployment):
        if not type(deployment) == AccidentDeployment:
            raise ValueError('not a AccidentDeployment object')

        self.job_queue.put(deployment)
        self.logger.debug('queue size=%s', self.job_queue.qsize())

    def consume(self):
        with RabbitMQ.setup_connection() as connection, connection.channel() as channel:
            channel.queue_declare(queue=self.queue_name)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(self.handle_rpc_call, queue=self.queue_name)

            self.logger.debug('binding to RabbitMQ with keys=%r', self.queue_name)

            update_thread = Thread(target=self.update, name='{}.update_thread'.format(self.object_name), daemon=True)

            def escape(signum, frame):
                raise StopConsuming

            signal.signal(signal.SIGINT, escape)
            signal.signal(signal.SIGTERM, escape)

            try:
                self.logger.debug('start update thread')
                update_thread.start()
                self.logger.debug('start consuming')
                channel.start_consuming()
            except StopConsuming:
                self.logger.debug('StopConsuming')
            except:
                self.logger.debug("unexpected error=%s", sys.exc_info()[0])
                self.logger.error(traceback.format_exc())
            finally:
                channel.stop_consuming()
                self.logger.debug('deleting queue')
                channel.queue_delete(queue=self.queue_name)
                self.logger.debug('closing connection')

        self.logger.debug('finishing')


class Status(Enum):
    EN_ROUTE = 'en_route'
    IDLE = 'idle'
    ASSESSING = 'assessing'
    BREAK = 'break'


class PlayerState:
    SPEED = 100

    condition = threading.Condition()

    def __init__(self, player_name, boundary):
        self.player_name = player_name
        self.boundary = boundary
        self.logger = setup_logging('{}.state'.format(player_name))

        self.status = Status.IDLE
        self.status_since = self.now()
        self.completion_time = self.now()

        self.lat = boundary.left + (random.betavariate(2, 2) * (boundary.right - boundary.left))
        self.long = boundary.bottom + (random.betavariate(2, 2) * (boundary.top - boundary.bottom))
        self.plan = None

        self.logger.debug('starting at lat=%s, long=%s', self.lat, self.long)
        self.logger.debug('initiated')

    @staticmethod
    def now():
        return datetime.utcnow().timestamp()

    def move_to(self, lat, long):
        self.condition.acquire()

        self.lat = lat
        self.long = long

        self.logger.debug('moving to lat=%r, long=%r', lat, long)

        self.condition.notify_all()
        self.condition.release()

    def current_location(self):
        self.condition.acquire()

        response = {'lat': self.lat, 'long': self.long}

        self.condition.notify_all()
        self.condition.release()

        return response

    def travel_to(self, plan):
        self.status = Status.EN_ROUTE
        self.status_since = self.now()
        self.plan = plan
        self.completion_time = self.now() + len(plan)

    def assess_accident(self):
        assert self.lat == self.plan[-1]['lat']['end']
        assert self.long == self.plan[-1]['long']['end']

        self.status = Status.ASSESSING
        self.status_since = self.now()
        self.plan = None
        assessing_time = round(random.normalvariate(15, 5), 1) * 60
        self.completion_time = self.now() + assessing_time

    def idle(self):
        self.status = Status.IDLE
        self.status_since = self.now()
        self.plan = None
        self.completion_time = self.now()

    def is_idle(self):
        return self.status == Status.IDLE

    def update(self):
        sec_lapsed = round(self.now() - self.status_since) * self.SPEED
        time_left = self.completion_time - self.now() - sec_lapsed
        self.logger.debug('status=%r, lapsed=%ssec, time_left=%ssec', self.status, sec_lapsed, time_left)

        if time_left <= 0:
            if self.status == Status.EN_ROUTE:
                self.logger.debug('start accessing accident')
                self.assess_accident()
            elif self.status == Status.ASSESSING:
                self.logger.debug('finish accessing accident')
                self.idle()
        else:
            if self.status == Status.EN_ROUTE:
                step = self.plan[sec_lapsed]
                self.logger.debug('step=%s', step)
                self.move_to(step['lat']['now'], step['long']['now'])


if __name__ == "__main__":
    retriever = GameMaster()
    retriever.serve()
