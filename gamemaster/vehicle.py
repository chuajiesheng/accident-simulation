import json
import math
import requests

from base import ServiceError

OSRM_DRIVING_URL = 'https://router.project-osrm.org/route/v1/driving/{src[long]},{src[lat]};{dest[long]},{dest[lat]}'


def split(durations, distances, coordinates, interval=1):
    len_durations = len(durations)
    len_distances = len(distances)

    assert len_durations == len_distances
    assert len_distances == len(coordinates)

    total_durations = sum(durations)
    total_distances = sum(distances)

    rolling_duration = [None] * len_durations
    for i in range(len_durations):
        rolling_duration[i] = round(sum(durations[0:i + 1]), 1)
    assert len(rolling_duration) == len_durations

    rolling_distance = [None] * len_distances
    for i in range(len_distances):
        rolling_distance[i] = round(sum(distances[0:i + 1]), 1)
    assert len(rolling_distance) == len_distances

    no_of_steps = round(rolling_duration[-1] * 10)
    steps = [None] * no_of_steps
    print('#steps=', len(steps))

    s = 0
    for i in range(no_of_steps):
        t = round(i * 0.1, 1)

        if t > rolling_duration[s + 1]:
            s += 1

        assert s < len_durations
        assert s < len_distances
        t_start = rolling_duration[s]
        t_end = rolling_duration[s + 1]
        t_gap = round(t_end - t_start, 3)

        d_start = rolling_distance[s]
        d_end = rolling_distance[s + 1]
        d_gap = round(d_end - d_start, 3)

        speed = d_gap / t_gap

        t_lapsed = t - t_start
        d_travelled = speed * t_lapsed
        d_now = round(d_start + d_travelled, 1 + 2)
        assert d_now <= d_end

        lat_start = coordinates[s][1]
        lat_end = coordinates[s + 1][1]
        lat_gap = round(lat_end - lat_start, 6 + 3)
        lat_speed = round(lat_gap / t_gap, 6 + 3)
        lat_now = round(lat_start + (lat_speed * t_lapsed), 6)
        if lat_end > lat_start:
            assert lat_now <= lat_end
        else:
            assert lat_now <= lat_start

        long_start = coordinates[s][0]
        long_end = coordinates[s + 1][0]
        long_gap = round(long_end - long_start, 6 + 3)
        long_speed = round(long_gap / t_gap, 6 + 3)
        long_now = round(long_start + (long_speed * t_lapsed), 6)
        if long_end > long_start:
            assert long_now <= long_end
        else:
            assert long_now <= long_start

        payload = {
            'time': t,
            'step': s,
            't': {
                'start': t_start,
                'end': t_end,
                'gap': t_gap,
                'speed': 0.1,
                'now': t
            },
            'd': {
                'start': d_start,
                'end': d_end,
                'gap': d_gap,
                'speed': speed,
                'now': d_now
            },
            'lat': {
                'start': lat_start,
                'end': lat_end,
                'gap': lat_gap,
                'speed': lat_speed,
                'now': lat_now
            },
            'long': {
                'start': long_start,
                'end': long_end,
                'gap': long_gap,
                'speed': long_speed,
                'now': long_now
            }
        }

        steps[i] = payload

    return [steps[i] for i in range(no_of_steps) if steps[i]['time'] % interval == 0]


def driving_by_car(src, dest, interval=1):
    url = OSRM_DRIVING_URL.format(src=src, dest=dest)
    querystring = {'overview': 'full', 'alternatives': 'false', 'steps': 'true', 'hints': '', 'geometries': 'geojson',
                   'annotations': 'true'}
    print('url=', url)

    headers = {
        'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:60.0) Gecko/20100101 Firefox/60.0",
        'Accept': "text/javascript",
        'Accept-Encoding': "gzip, deflate, br",
        'Accept-Language': "en-US,en;q=0.5",
        'X-Requested-With': "XMLHttpRequest",
        'Cache-Control': "no-cache"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    if response.status_code != 200:
        raise ServiceError('HTTP {}'.format(response.status_code))

    data = response.text
    payload = json.loads(data, encoding='utf-8')

    coordinates = payload['routes'][0]['geometry']['coordinates']
    print('#coordinates=', len(coordinates))
    # print(coordinates)

    legs = payload['routes'][0]['legs']
    print('#legs=', len(legs))

    expected_duration = payload['routes'][0]['duration']
    expected_distance = payload['routes'][0]['distance']

    for leg in legs:
        annotation = leg['annotation']
        distance = leg['distance']
        duration = leg['duration']

        assert duration == expected_duration
        assert distance == expected_distance

        distance_breakdown = annotation['distance']
        distance_breakdown.insert(0, 0)

        duration_breakdown = annotation['duration']
        duration_breakdown.insert(0, 0)

        print('#distance_breakdown=', len(distance_breakdown))
        print('#duration_breakdown=', len(duration_breakdown))

        by_interval = split(distance_breakdown, duration_breakdown, coordinates, interval)
        print(by_interval)
        print('#by_interval=', len(by_interval))

        leg_coordinates = []
        leg_total_distance = 0
        leg_total_duration = 0

        for step in leg['steps']:
            coordinates = step['geometry']['coordinates']
            duration = float(step['duration'])
            distance = float(step['distance'])

            if len(leg_coordinates) == 0:
                leg_coordinates.extend(coordinates)
            elif distance == 0:
                continue
            else:
                leg_coordinates.extend(coordinates[1:])

            leg_total_distance += float(distance)
            leg_total_duration += float(duration)

        print('#leg_coordinates=', len(leg_coordinates))
        print('#leg_total_distance=', leg_total_distance)
        print('#leg_total_duration=', leg_total_duration)

        # print(leg_coordinates)

    print('expected_duration=', expected_duration)
    print('expected_distance=', expected_distance)


src = {
    'lat': '1.4361274',
    'long': '103.7721054'
}
dest = {
    'lat': '1.2781208',
    'long': '103.850805851355'
}
driving_by_car(src, dest, 5)
