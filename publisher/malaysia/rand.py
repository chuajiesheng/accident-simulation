import json

from publisher.core import RandomAccidentRetriever, Boundary
from core import setup_logging


class KlangValleyRandomAccidentRetriever(RandomAccidentRetriever):
    def __init__(self):
        boundary = Boundary(100.711638, 3.870733, 101.970674, 2.533530)
        interval = 5
        super(KlangValleyRandomAccidentRetriever, self).__init__(boundary, interval)
        self.logger = setup_logging('KlangValleyRandomAccidentRetriever')

    def publish(self, payload):
        routing_key = 'malaysia.klang_valley'
        message = json.dumps(payload.to_dict())
        self.channel.basic_publish(exchange='accidents', routing_key=routing_key, body=message)
        self.logger.debug("Sent routing_key=%r message=%r".format(routing_key, message))


if __name__ == "__main__":
    retriever = KlangValleyRandomAccidentRetriever()
    retriever.watch()

