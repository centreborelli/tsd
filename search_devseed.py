#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Landsat images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

from __future__ import print_function
import os
import argparse
import datetime
import json
import requests
import shapely.geometry
import numpy as np

import utils


api_url = 'https://api.developmentseed.org/satellites/landsat'
s2_mgrs_grid = os.path.join(os.path.dirname(os.path.abspath(__file__)), 's2_mgrs_grid.txt')


def query_l8(lat, lon, start_date=None, end_date=None):
    """
    Build a search string to query the Development Seed API for Landsat-8 data.

    Args:
        lat, lon: latitude, longitude
        start_date, end_date: datetime.date objects

    Returns:
        string
    """
    x = 'satellite_name:landsat-8'

    # position
    x += '+AND+upperLeftCornerLatitude:[{}+TO+1000]'.format(lat)
    x += '+AND+lowerRightCornerLatitude:[-1000+TO+{}]'.format(lat)
    x += '+AND+lowerLeftCornerLongitude:[-1000+TO+{}]'.format(lon)
    x += '+AND+upperRightCornerLongitude:[{}+TO+1000]'.format(lon)

    # date range
    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)
    x += '+AND+acquisitionDate:[{}+TO+{}]'.format(start_date.isoformat(),
                                                  end_date.isoformat())
    return x


def bytes2str(x):
    """
    """
    return np.str(x.decode())


def parse_s2_tiling_grid(lon, lat, grid=s2_mgrs_grid):
    """
    Search in the sentinel-2 tiling grid a MGRS tile containing a given point.

    Args:
        lat, lon: geographic coordinates of the input geographic location

    Returns:
        The MGRS identifier of a tile. It's a string of lenght 5. The first two
        characters indicate the utm code ranging from 01 to 60 (indicating a
        longitude band), the next is an uppercase letter indicating the utm
        latitude band, and the last two are two uppercase letters giving a
        100,000-meter square MGRS identifier.
    """
    # load the list of lon lat bounding boxes of MGRS tiles used by sentinel-2
    mgrsid = np.loadtxt(grid, usecols=[0], converters={0:bytes2str}, dtype=np.str)
    ll_bbx = np.loadtxt(grid, usecols=[1, 2, 3, 4])

    # search the ones for which our point is inside the bounding box
    idx = np.logical_and(np.logical_and(ll_bbx[:, 0] < lon,
                                        ll_bbx[:, 1] > lon),
                         np.logical_and(ll_bbx[:, 2] < lat,
                                        ll_bbx[:, 3] > lat))
    mgrsid = mgrsid[idx].tolist()

    if not mgrsid:
        print('WARNING: lat, lon ({}, {}) not located in any tile'.format(lat, lon))

    return mgrsid


def mgrs_id_query_string(m):
    """
    """
    return '(utm_zone:{}+AND+latitude_band:{}+AND+grid_square:{})'.format(m[:2],
                                                                          m[2],
                                                                          m[3:])

def query_s2(lat, lon, start_date=None, end_date=None):
    """
    Build a search string to query the Development Seed API for Sentinel-2 data.

    Args:
        lat, lon: latitude, longitude
        start_date, end_date: datetime.date objects

    Returns:
        string
    """
    x = 'satellite_name:sentinel-2'

    # relevant MGRS tiles
    x += '+AND+({})'.format('+OR+'.join(mgrs_id_query_string(x) for x in
                                        parse_s2_tiling_grid(lon, lat)))

    # date range
    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)
    x += '+AND+date:[{}+TO+{}]'.format(start_date.isoformat(),
                                       end_date.isoformat())
    return x


def search(aoi, start_date=None, end_date=None, satellite='Landsat-8'):
    """
    List images covering an area of interest (AOI) using Development Seed’s API.

    Args:
        aoi: geojson.Polygon or geojson.Point object
        satellite: either Landsat-8 or Sentinel-2
    """
    # compute the centroid of the area of interest
    lon, lat = shapely.geometry.shape(aoi).centroid.coords.xy
    lon, lat = lon[0], lat[0]

    # build url
    if satellite == 'Landsat-8':
        search_string = query_l8(lat, lon, start_date, end_date)
    elif satellite == 'Sentinel-2':
        search_string = query_s2(lat, lon, start_date, end_date)
    url = '{}?search={}&limit=1000'.format(api_url, search_string)

    # query Development Seed’s API
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
        if 'data_geometry' in x:
            if not shapely.geometry.shape(x['data_geometry']).contains(aoi):
                not_covering.append(x)

    for x in not_covering:
        d['results'].remove(x)
        d['meta']['found'] -= 1

    # remove 'crs' fields to make the json dict compatible with geojsonio
    if satellite == 'Landsat-8':
        for x in d['results']:
            x['data_geometry'].pop('crs')

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
