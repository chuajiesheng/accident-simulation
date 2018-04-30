import time
from collections import defaultdict, namedtuple
import argparse
import requests
import json
from datetime import datetime, timedelta

from publisher.core import AccidentRetriever
from base import ServiceError, ServicePayloadError, AccidentLocation, Boundary, AccidentPayload
from base import setup_logging


class GreaterBangkokAccidentRetriever(AccidentRetriever):

    def __init__(self, year):
        boundary = Boundary(13.381014, 100.950397, 14.284958, 99.869143)
        interval = 60 * 1
        super(GreaterBangkokAccidentRetriever, self).__init__(boundary, interval)
        self.logger = setup_logging('GreaterBangkokAccidentRetriever')
        
        self.year = year
        self.logger.debug('using year=%s', year)

    @staticmethod
    def get_alerts(payload):
        url = "http://www.thairsc.com/services/GetAmphurTopThreeList"

        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:60.0) Gecko/20100101 Firefox/60.0",
            'Accept': "text/javascript",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "en-US,en;q=0.5",
            'Accept-Encoding': "gzip, deflate",
            'Referer': "http://www.thairsc.com/p77/index/10",
            'Cache-Control': "no-cache",
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            raise ServiceError('HTTP {}'.format(response.status_code))

        data = response.text
        payload = json.loads(data, encoding='utf-8')

        if 'dtLatlng' not in payload.keys():
            raise ServicePayloadError('Missing dtLatlng attributes.')

        return payload['dtLatlng']

    def watch(self):
        Timestamp = namedtuple('Timestamp', ['month', 'day', 'hour', 'min'])
        accident_lookup = defaultdict(list)

        alerts = {
            'samut_sakhon': self.get_alerts('provid=%E0%B8%AA%E0%B8%9B&ampid=1&years={}'.format(self.year)),
            'nakhon_pathom': self.get_alerts('provid=%E0%B8%99%E0%B8%90&ampid=1&years={}'.format(self.year)),
            'nonthaburi': self.get_alerts('provid=%E0%B8%99%E0%B8%9A&ampid=1&years={}'.format(self.year)),
            'bangkok': self.get_alerts('provid=%E0%B8%81%E0%B8%97&ampid=1&years={}'.format(self.year)),
            'pathum_thani': self.get_alerts('provid=%E0%B8%9B%E0%B8%97&ampid=1&years={}'.format(self.year)),
            'samutprakan': self.get_alerts('provid=%E0%B8%AA%E0%B8%9B&ampid=1&years={}'.format(self.year)),
        }

        for k in alerts:
            area = alerts[k]
            self.logger.debug('#accident in %s=%s', k, len(area))
            for i in range(len(area)):
                accident = area[i]
                utc_accdate = datetime.strptime(accident['accdate'], "%d/%m/%Y %H:%M").replace(year=datetime.now().year) - timedelta(hours=7)
                utc_timestamp = utc_accdate.timestamp()

                timestamp = Timestamp(month=utc_accdate.month, day=utc_accdate.day,
                                      hour=utc_accdate.hour, min=utc_accdate.minute)
                lat = accident['lat']
                long = accident['lng']
                accident_payload = AccidentPayload(self.boundary, AccidentLocation(lat, long), utc_timestamp)
                accident_lookup[timestamp].append(accident_payload)

        try:
            while True:
                now = datetime.utcnow()
                timestamp = Timestamp(month=now.month, day=now.day, hour=now.hour, min=now.minute)
                accidents = alerts.get(timestamp) or []
                self.logger.debug('timestamp=%r, #accidents=%s', timestamp, len(accidents))

                for i in accidents:
                    self.publish(accidents[i])

                self.logger.debug('Sleep secs=%s', self.interval)
                time.sleep(self.interval)
        except KeyboardInterrupt:
            self.logger.debug('KeyboardInterrupt')

    def publish(self, payload):
        routing_key = 'thailand.greater_bangkok'
        message = json.dumps(payload.to_dict())
        self.channel.basic_publish(exchange='accidents', routing_key=routing_key, body=message)
        self.logger.debug("Sent routing_key=%r message=%r", routing_key, message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("year", help='use the accident dataset from the specific year. if not integer provided, default to 2017')
    args = parser.parse_args()
    try:
        year = int(args.year)
    except ValueError:
        year = 2017

    retriever = GreaterBangkokAccidentRetriever(year)
    retriever.watch()

