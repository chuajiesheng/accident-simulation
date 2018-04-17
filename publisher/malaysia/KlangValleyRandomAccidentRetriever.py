from publisher.core import RandomAccidentRetrieve, AccidentRetriever, ServiceError, ServicePayloadError, Boundary, AccidentPayload, AccidentLocation
import time
import requests
import pika
import json


class KlangValleyRandomAccidentRetrieve(RandomAccidentRetrieve):
    def __init__(self):
        boundary = Boundary(100.711638, 3.870733, 101.970674, 2.533530)
        interval = 5
        super(KlangValleyRandomAccidentRetrieve, self).__init__(boundary, interval)

    @staticmethod
    def publish(payload):
        routing_key = 'malaysia.klang_valley'
        message = json.dumps(payload.to_dict())
        self.channel.basic_publish(exchange='accidents', routing_key=routing_key, body=message)
        print(" [x] Sent %r:%r" % (routing_key, message))


if __name__ == "__main__":
    retriever = KlangValleyRandomAccidentRetrieve()
    retriever.watch()

