#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Landsat-8 and Sentinel-2 images using Development Seed API.

Copyright (C) 2016-18, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

import argparse
import datetime
import json
import requests
import shapely.geometry
import geojson
import urllib.parse

import utils


API_URL = 'https://sat-api.developmentseed.org/search/stac'


def query_sat_api(satellite, aoi, start_date=None, end_date=None):
    """
    Build a search string to query the Development Seed sat-api API.

    Args:
        satellite (string): either 'sentinel-2' or 'landsat-8'
        aoi ():
        start_date, end_date: datetime.date objects

    Returns:
        string
    """
    x = 'c:id="{}"'.format(satellite)

    # date range
    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)
    x += '&datetime={}/{}'.format(start_date.isoformat(), end_date.isoformat())

    # area of interest
    # replace whitespaces with '+' in the aoi string, then url encode it except
    # for the '+' otherwise it doesn't work... don't know why
    aoi_string = geojson.dumps(aoi).replace(' ', '+')
    aoi_string = urllib.parse.quote(aoi_string).replace('%2B', '+')

    # currently sat-api supports only 'intersect' requests
    x += '&intersects={}'.format(aoi_string)

    return x


def search(aoi, start_date=None, end_date=None, satellite='Landsat-8'):
    """
    List images covering an area of interest (AOI) using Development Seed’s API.

    Args:
        aoi: geojson.Polygon or geojson.Point object
        satellite: either Landsat-8 or Sentinel-2
    """
    # build url
    search_string = query_sat_api(satellite.lower(), aoi, start_date, end_date)
    url = '{}?limit=1000&{}'.format(API_URL, search_string)

    # query Development Seed’s API
    r = requests.get(url)
    if r.ok:
        d = r.json()
    else:
        print('WARNING: request to {} returned {}'.format(url, r.status_code))
        return

    # check if the image footprint contains the area of interest
    to_remove = set()
    aoi = shapely.geometry.shape(aoi)
    for i, x in enumerate(d['features']):
        if 'geometry' in x:
            if not shapely.geometry.shape(x['geometry']).contains(aoi):
                to_remove.add(i)

    for i in sorted(to_remove, reverse=True):  # delete the higher index first
        del d['features'][i]
        d['properties']['found'] -= 1

    return d


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search of Landsat-8 and Sentinel-2 images.')
    parser.add_argument('--satellite', choices=['Landsat-8', 'Sentinel-2'],
                        help=('either "Landsat-8" or "Sentinel-2"'),
                        default='Landsat-8')
    parser.add_argument('--geom', type=utils.valid_geojson,
                        help=('path to geojson file'))
    parser.add_argument('--lat', type=utils.valid_lat,
                        help=('latitude of the center of the rectangle AOI'))
    parser.add_argument('--lon', type=utils.valid_lon,
                        help=('longitude of the center of the rectangle AOI'))
    parser.add_argument('-w', '--width', type=int, default=5000,
                        help='width of the AOI (m), default 5000 m')
    parser.add_argument('-l', '--height', type=int, default=5000,
                        help='height of the AOI (m), default 5000 m')
    parser.add_argument('-s', '--start-date', type=utils.valid_datetime,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_datetime,
                        help='end date, YYYY-MM-DD')
    args = parser.parse_args()

    if args.geom and (args.lat or args.lon):
        parser.error('--geom and {--lat, --lon} are mutually exclusive')

    if not args.geom and (not args.lat or not args.lon):
        parser.error('either --geom or {--lat, --lon} must be defined')

    if args.geom:
        aoi = args.geom
    else:
        aoi = utils.geojson_geometry_object(args.lat, args.lon, args.width,
                                            args.height)

    print(json.dumps(search(aoi, start_date=args.start_date,
                            end_date=args.end_date, satellite=args.satellite)))
