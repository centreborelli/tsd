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
import json
import requests
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


def search(aoi, start_date=None, end_date=None):
    """
    List the L8 images covering a location using Development Seed’s Landsat API.

    Args:
        aoi: geojson.Polygon or geojson.Point object
    """
    # compute the centroid of the area of interest
    lon, lat = shapely.geometry.shape(aoi).centroid.coords.xy
    lon, lat = lon[0], lat[0]

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

    # check if the image footprint contains the area of interest
    aoi = shapely.geometry.shape(aoi)
    not_covering = []
    for x in d['results']:
        if not shapely.geometry.shape(x['data_geometry']).contains(aoi):
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
    parser.add_argument('--geom', type=utils.valid_geojson,
                        help=('path to geojson file'))
    parser.add_argument('--lat', type=utils.valid_lat,
                        help=('latitude of the center of the rectangle AOI'))
    parser.add_argument('--lon', type=utils.valid_lon,
                        help=('longitude of the center of the rectangle AOI'))
    parser.add_argument('-w', '--width', type=int, help='width of the AOI (m)')
    parser.add_argument('-l', '--height', type=int, help='height of the AOI (m)')
    parser.add_argument('-s', '--start-date', type=utils.valid_datetime,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_datetime,
                        help='end date, YYYY-MM-DD')
    args = parser.parse_args()

    if args.geom and (args.lat or args.lon or args.width or args.height):
        parser.error('--geom and {--lat, --lon, -w, -l} are mutually exclusive')

    if not args.geom and (not args.lat or not args.lon):
        parser.error('either --geom or {--lat, --lon} must be defined')

    if args.geom:
        aoi = args.geom
    else:
        aoi = utils.geojson_geometry_object(args.lat, args.lon, args.width,
                                            args.height)

    print(json.dumps(search(aoi, start_date=args.start_date,
                            end_date=args.end_date)))
