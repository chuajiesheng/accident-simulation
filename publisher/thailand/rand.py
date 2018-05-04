import json

from publisher.core import RandomAccidentRetriever
from base import setup_logging, Boundary


class GreaterBangkokRandomAccidentRetriever(RandomAccidentRetriever):
    def __init__(self):
        boundary = Boundary(14.197410, 100.349791, 13.532420, 100.882992)
        interval = 10
        super(GreaterBangkokRandomAccidentRetriever, self).__init__(boundary, interval)
        self.logger = setup_logging('GreaterBangkokRandomAccidentRetriever')

    def publish(self, payload):
        routing_key = 'thailand.greater_bangkok'
        message = json.dumps(payload.to_dict())
        self.channel.basic_publish(exchange='accidents', routing_key=routing_key, body=message)
        self.logger.debug("Sent routing_key=%r message=%r", routing_key, message)


if __name__ == "__main__":
    retriever = GreaterBangkokRandomAccidentRetriever()
    retriever.watch()

