# -*- coding: utf-8 -*-
"""
Created on Thu Dec 08 09:43:48 2018

@author: v.radchenko, v.shkaberda
"""

from collections import namedtuple
from datetime import timedelta
from db_connect import DBConnect

import requests
import time

ERROR_COUNT = 0  # Counter of raised Value Errors


def not_valid_response(text):
    ''' Function to catch API errors or empty responce.
    '''
    if not text:
        return True
    if text.find('Fatal error') != -1:
        return True
    if text.find('unexpected error') != -1:
        return True


def get_km_time(n, latA, lonA, latB, lonB, id_=None):
    ''' Returns km and time.
    Input: n - 1 selects the fastest route, 0 the shortest route; coordinates.
    Output: (km, time).
    '''
    global ERROR_COUNT
    if latA == latB and lonA == lonB:
        return (0, 0)  # the same object geographically
    url = f'http://www.yournavigation.org/api/1.0/gosmore.php?format=kml&flat={latA}&' + \
          f'flon={lonA}&tlat={latB}&tlon={lonB}&' + \
          f'v=motorcar&fast={n}&layer=mapnik&format=geojson'
    response = requests.get(url, timeout=15)
    assert response.status_code == 200, 'Response status is {}'.format(response.status_code)
    try:
        data = response.json()
    except ValueError:  # sometimes we have invalid response
        # if response is empty or API error
        if ERROR_COUNT < 10 and not_valid_response(response.text):
            ERROR_COUNT += 1
            raise
        with open('JSONerrors.txt', 'a') as f:
            f.write('{} {}\n'.format(id_, response.text))
        ERROR_COUNT = 0
        return (0, 0)
    return (data['properties']['distance'], data['properties']['traveltime'])


def geoYN(args, db_params):
    ''' Function that provides data translation between functions
        and contains main cycle for performing queries to API and
        updating data on sereve.
    '''
    # storage for distance and travel time
    km_time = [(0, 0), (0, 0)]
    kt = namedtuple('kt', ['km', 'time'])

    with DBConnect(**db_params) as sql:
        if args.count:
            print("Кол-во маршрутов без расстояния: ", sql.count_empty_rows())
            return

    start = time.localtime()
    print('Script started at {}.'.format(time.strftime("%d-%m-%Y %H:%M", start)))

    while True:
        try:
            with DBConnect(**db_params) as sql:
                # Row without distance
                row = sql.empty_dist()
                if not row:
                    break  # if no rows - we're done

                id_, _pointA, latA, lonA, _pointB, latB, lonB = row

                # YN_fast = 1 selects the fastest route, 0 the shortest route
                for n in (0, 1):
                    km_time[n] = kt(*get_km_time(n, latA, lonA, latB, lonB, id_))

                sql.update_dist(id_, km_time)

                # tracking because script appeares to be frozen sometimes
                print("id = {}; route_km = {}; time = {}; trips_km = {}; time = {}"
                       .format(id_, *km_time[1], *km_time[0]))
        except Exception as e:
            print('Row: ', row)
            print(e)
            time.sleep(3)

    end = time.localtime()
    print('Script ended at {}.'.format(time.strftime("%d-%m-%Y %H:%M", end)))
    print('Total duration: {}.'.format(timedelta(seconds=time.mktime(end)-time.mktime(start))))


if __name__ == '__main__':
    data = get_km_time(1, 50.543215, 30.825806, 50.502558, 30.605179)
    msg = 'Please, ensure that {} returned by API is correct'
    assert float(data[0]) > 20, msg.format('distance')
    assert int(data[1]) > 100, msg.format('time')
    input('End\nPress "Enter" to exit...')