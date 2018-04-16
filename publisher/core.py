import random
import time
import json


class AccidentRetriever:
    def __init__(self):
        pass

    def watch(self):
        raise NotImplementedError

    @staticmethod
    def publish(payload):
        raise NotImplementedError


class RandomAccidentRetrieve(AccidentRetriever):
    def __init__(self, boundary, interval):
        self.boundary = boundary
        self.interval = interval
        self.r = random.Random(42)

    def watch(self):
        if self.boundary.top > self.boundary.bottom:
            lat = random.uniform(self.boundary.bottom, self.boundary.top)
        else:
            lat = random.uniform(self.boundary.top, self.boundary.bottom)

        if self.boundary.left > self.boundary.right:
            long = random.uniform(self.boundary.right, self.boundary.left)
        else:
            long = random.uniform(self.boundary.left, self.boundary.right)

        while True:
            lat = self.boundary.bottom + self.r.random() * (self.boundary.top - self.boundary.bottom)
            long = self.boundary.left + self.r.random() * (self.boundary.right - self.boundary.left)

            self.publish(AccidentPayload(self.boundary, AccidentLocation(lat, long)))
            time.sleep(self.interval)

    @staticmethod
    def publish(payload):
        raise NotImplementedError


class Boundary:
    def __init__(self, left, top, right, bottom):
        self.left = left
        self.top = top
        self.right = right
        self.bottom = bottom

    def to_dict(self):
        return {
            'left': self.left,
            'top': self.top,
            'right': self.right,
            'bottom': self.bottom
        }


class AccidentLocation:
    def __init__(self, lat, long):
        self.lat = lat
        self.long = long

    def to_dict(self):
        return {
            'lat': self.lat,
            'long': self.long
        }


class AccidentPayload:
    def __init__(self, boundary, location):
        self.boundary = boundary
        self.location = location

    def to_dict(self):
        return {
            'boundary': self.boundary.to_dict(),
            'accident': self.location.to_dict()
        }


class ServiceError(Exception):
    pass


class ServicePayloadError(Exception):
    pass
