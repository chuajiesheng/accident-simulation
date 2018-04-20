from publisher.core import RandomAccidentRetriever, Boundary
import json


class KlangValleyRandomAccidentRetriever(RandomAccidentRetriever):
    def __init__(self):
        boundary = Boundary(100.711638, 3.870733, 101.970674, 2.533530)
        interval = 5
        super(KlangValleyRandomAccidentRetriever, self).__init__(boundary, interval)

    def publish(self, payload):
        routing_key = 'malaysia.klang_valley'
        message = json.dumps(payload.to_dict())
        self.channel.basic_publish(exchange='accidents', routing_key=routing_key, body=message)
        print(" [x] Sent %r:%r" % (routing_key, message))


if __name__ == "__main__":
    retriever = KlangValleyRandomAccidentRetriever()
    retriever.watch()

