import time
import queue
import uuid
from datetime import datetime

import pika
import json
import functools

from mq import RabbitMQ
from base import setup_logging, StoppableThread, deserialize_message, AccidentPayload, AccidentDeployment, RpcCall, \
    PlayerInstruction, DeferDecision


class DeploymentMaster:
    PLAYER_RPC_TIMEOUT_SECS = 10
    boundary = None

    @staticmethod
    def _on_response(rpc, logger, ch, method, props, body):
        if rpc.correlation_id == props.correlation_id:
            rpc.body = body
            logger.debug('on_response, response=%r', body)

    @staticmethod
    def _on_timeout(rpc, logger):
        rpc.error = True
        logger.debug('rpc timeout')

    def __init__(self, binding_keys):
        self.logger = setup_logging('DeploymentMaster')

        self.message_queue = queue.Queue()
        self.accident_event_consumer = AccidentEventConsumer(self.message_queue, binding_keys)

        self.team_uuid = str(uuid.uuid4())
        self.player_count = 5

        self.logger.debug('initiated')

    def game_master_rpc(self, payload):
        with RabbitMQ.setup_connection() as connection, connection.channel() as channel:
            result = channel.queue_declare(exclusive=True)
            callback_queue = result.method.queue

            corr_id = str(uuid.uuid4())
            rpc = RpcCall(corr_id)

            connection.add_timeout(self.PLAYER_RPC_TIMEOUT_SECS, functools.partial(self._on_timeout, rpc, self.logger))
            channel.basic_consume(functools.partial(self._on_response, rpc, self.logger), no_ack=True, queue=callback_queue)
            channel.basic_publish(exchange='',
                                  routing_key=RabbitMQ.game_master_queue_name(),
                                  properties=pika.BasicProperties(reply_to=callback_queue,
                                                                  correlation_id=corr_id),
                                  body=json.dumps(payload))

            self.logger.debug('waiting to process event')
            while not rpc.completed():
                connection.process_data_events()

            self.logger.debug('error=%s, response=%r', rpc.error, rpc.body)
            return rpc.body

    def start(self):
        self.logger.debug('request team')
        self.game_master_rpc({
            'type': 'request',
            'team_uuid': self.team_uuid,
            'player_count': self.player_count,
            'team_boundary': self.boundary.to_dict()
        })

        self.logger.debug('starting deployment manager')
        self.accident_event_consumer.start()

        try:
            while True:
                time.sleep(5)
                self.logger.debug('deployment manager status=%s', self.accident_event_consumer.is_alive())
                self.logger.debug('empty_queue=%s', self.message_queue.empty())
                try:
                    msg = self.message_queue.get(block=False)
                    self.logger.debug("message=%r", msg)
                    self.decide(AccidentPayload.from_dict(msg))
                except queue.Empty:
                    continue

        except KeyboardInterrupt:
            self.logger.debug('KeyboardInterrupt')
        finally:
            self.logger.debug('stopping player processes')
            self.game_master_rpc({
                'type': 'terminate',
                'team_uuid': self.team_uuid,
            })
            self.logger.debug('stopping deployment manager')
            self.accident_event_consumer.stop()
            self.accident_event_consumer.join()

    def decide(self, payload):
        raise NotImplemented

    def deploy(self, player, payload):
        action = PlayerInstruction.GO
        deployment_decision = AccidentDeployment(action, payload).to_dict()
        return self._rpc(player, deployment_decision)

    def ask(self, player, action):
        payload = {
            'action': action.value,
            'utc_decision_time': datetime.utcnow().timestamp(),
        }
        return self._rpc(player, payload)

    def _rpc(self, player, dict_payload):
        player_queue = '{}.player{}'.format(self.team_uuid, player)
        self.logger.debug('rpc-ing player%s %r', player, dict_payload)

        with RabbitMQ.setup_connection() as connection, connection.channel() as channel:
            result = channel.queue_declare(exclusive=True)
            callback_queue = result.method.queue

            corr_id = str(uuid.uuid4())
            rpc = RpcCall(corr_id)

            connection.add_timeout(self.PLAYER_RPC_TIMEOUT_SECS, functools.partial(self._on_timeout, rpc, self.logger))
            channel.basic_consume(functools.partial(self._on_response, rpc, self.logger), no_ack=True, queue=callback_queue)
            channel.basic_publish(exchange='',
                                  routing_key=player_queue,
                                  properties=pika.BasicProperties(reply_to=callback_queue,
                                                                  correlation_id=corr_id),
                                  body=json.dumps(dict_payload))

            self.logger.debug('waiting to process event')
            while not rpc.completed():
                connection.process_data_events()

            self.logger.debug('error=%s, response=%r', rpc.error, rpc.body)

        return rpc


class AccidentEventConsumer(StoppableThread):
    def __init__(self, message_queue, binding_keys):
        super(AccidentEventConsumer, self).__init__()
        self.message_queue = message_queue
        self.logger = setup_logging('AccidentEventConsumer')
        self.binding_keys = binding_keys

    def consume(self):
        connection = RabbitMQ.setup_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=RabbitMQ.accident_exchange_name(), exchange_type='topic')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        for binding_key in self.binding_keys:
            channel.queue_bind(exchange=RabbitMQ.accident_exchange_name(), queue=queue_name, routing_key=binding_key)

        self.logger.debug('binding to RabbitMQ')

        channel.basic_consume(self.process, queue=queue_name, no_ack=True)
        channel.start_consuming()

    def process(self, channel, method, properties, body):
        self.logger.debug('method.routing_key=%s; body=%s;', method.routing_key, body)
        self.message_queue.put(deserialize_message(body))

        if self.stopped():
            channel.stop_consuming()
            self.logger.debug('stop consuming')
