#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Landsat images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

from __future__ import print_function
import argparse
import datetime
import requests
import json
import shapely.geometry

import utils


api_url = 'https://api.developmentseed.org/satellites/landsat'


def query_builder(lat, lon, start_date=None, end_date=None):
    """
    Build the search string to query the Development Seed Landsat API.

    Args:
        lat, lon: latitude, longitude
        start_date, end_date: datetime.date objects

    Returns:
        string
    """
    # default start/end dates
    if start_date is None:
        start_date = datetime.date(2013, 7, 1)
    if end_date is None:
        end_date = datetime.date.today()

    # position
    x = 'upperLeftCornerLatitude:[{}+TO+1000]'.format(lat)
    x += '+AND+lowerRightCornerLatitude:[-1000+TO+{}]'.format(lat)
    x += '+AND+lowerLeftCornerLongitude:[-1000+TO+{}]'.format(lon)
    x += '+AND+upperRightCornerLongitude:[{}+TO+1000]'.format(lon)

    # date range
    x += '+AND+acquisitionDate:[{}+TO+{}]'.format(start_date.isoformat(),
                                                  end_date.isoformat())
    return x


def search(lat, lon, w=None, h=None, start_date=None, end_date=None):
    """
    List the L8 images covering a location using Development Seed’s Landsat API.
    """
    # build url
    search_string = query_builder(lat, lon, start_date, end_date)
    url = '{}?search={}&limit=1000'.format(api_url, search_string)

    # query Development Seed’s Landsat API
    r = requests.get(url)
    if r.ok:
        d = r.json()
    else:
        print('WARNING: request to {} returned {}'.format(url, r.status_code))
        return

    # check if the image footprint contains the point or region of interest (roi)
    if w is not None and h is not None:
        roi = shapely.geometry.Polygon(utils.lonlat_rectangle_centered_at(lon, lat, w, h))
    else:
        roi = shapely.geometry.Point(lon, lat)

    not_covering = []
    for x in d['results']:
        if not shapely.geometry.shape(x['data_geometry']).contains(roi):
            not_covering.append(x)

    for x in not_covering:
        d['results'].remove(x)
        d['meta']['found'] -= 1

    # remove 'crs' fields to make the json dict compatible with geojsonio
    for x in d['results']:
        x['data_geometry'].pop('crs')

    return d


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search of Landsat-8 images.')
    parser.add_argument('--lat', type=utils.valid_lat, required=True,
                        help=('latitude of the interest point'))
    parser.add_argument('--lon', type=utils.valid_lon, required=True,
                        help=('longitude of the interest point'))
    parser.add_argument('-w', '--width', type=int, help='width of the area (m)')
    parser.add_argument('-l', '--height', type=int, help='height of the area (m)')
    parser.add_argument('-s', '--start-date', type=utils.valid_date,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_date,
                        help='end date, YYYY-MM-DD')
    args = parser.parse_args()

    print(json.dumps(search(args.lat, args.lon, args.width, args.height,
                            start_date=args.start_date,
                            end_date=args.end_date)))
