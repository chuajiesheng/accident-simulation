import json

from publisher.core import RandomAccidentRetriever
from base import setup_logging, Boundary


class KlangValleyRandomAccidentRetriever(RandomAccidentRetriever):
    def __init__(self):
        boundary = Boundary(3.870733, 100.711638, 2.533530, 101.970674)
        interval = 5
        super(KlangValleyRandomAccidentRetriever, self).__init__(boundary, interval)
        self.logger = setup_logging('KlangValleyRandomAccidentRetriever')

    def publish(self, payload):
        routing_key = 'malaysia.klang_valley'
        message = json.dumps(payload.to_dict())
        self.channel.basic_publish(exchange='accidents', routing_key=routing_key, body=message)
        self.logger.debug("Sent routing_key=%r message=%r", routing_key, message)


if __name__ == "__main__":
    retriever = KlangValleyRandomAccidentRetriever()
    retriever.watch()

