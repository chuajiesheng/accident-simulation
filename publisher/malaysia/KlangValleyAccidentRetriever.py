from publisher.core import RandomAccidentRetrieve, AccidentRetriever, ServiceError, ServicePayloadError, Boundary, AccidentPayload, AccidentLocation
import time
import requests
import pika
import json


class KlangValleyAccidentRetriever(AccidentRetriever):
    TEN_MINUTES = 1000 * 60 * 10
    EIGHT_HOURS = 1000 * 60 * 60 * 8
    TO_MS = 1000

    def __init__(self):
        boundary = Boundary(100.711638, 3.870733, 101.970674, 2.533530)
        interval = 60 * 10
        super(KlangValleyAccidentRetriever, self).__init__(boundary, interval)

    @staticmethod
    def get_alerts():
        url = "https://www.waze.com/row-rtserver/web/TGeoRSS"
        querystring = {'ma': '600', 'mj': '100', 'mu': '100', 'left': '100.711638', 'right': '101.970674', 'bottom': '2.533530',
             'top': '3.870733', '_': '1523637203329'}

        headers = {
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:60.0) Gecko/20100101 Firefox/60.0",
            'Accept': "text/javascript",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "en-US,en;q=0.5",
            'Referer': "https://www.waze.com/livemap",
            'X-Requested-With': "XMLHttpRequest",
            'Cache-Control': "no-cache"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        if response.status_code != 200:
            raise ServiceError('HTTP {}'.format(response.status_code))

        data = response.text
        payload = json.loads(data, encoding='utf-8')

        if 'alerts' not in payload.keys():
            raise ServicePayloadError('Missing dtLatlng attributes.')

        return payload['alerts']

    def watch(self):
        while True:
            alerts = self.get_alerts()
            map(self.handle_alert, alerts)

    def handle_alert(self, alert):
        alert_time_millis = alert['pubMillis']
        now_millis = int(round(time.time() * self.TO_MS)) + self.EIGHT_HOURS
        delta_millis = now_millis - alert_time_millis

        lat = alert['location']['y']
        long = alert['location']['x']

        if alert['type'] == 'ACCIDENT' and delta_millis < (self.interval * self.TO_MS):
            self.publish(AccidentPayload(self.boundary, AccidentLocation(lat, long)))
            time.sleep(self.interval)

    def publish(self, payload):
        routing_key = 'malaysia.klang_valley'
        message = json.dumps(payload.to_dict())
        self.channel.basic_publish(exchange='accidents', routing_key=routing_key, body=message)
        print(" [x] Sent %r:%r" % (routing_key, message))


if __name__ == "__main__":
    retriever = KlangValleyAccidentRetriever()
    retriever.watch()

