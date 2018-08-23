#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Landsat-8 and Sentinel-2 images using Development Seed API.

Copyright (C) 2016-18, Carlo de Franchis <carlo.de-franchis@m4x.org>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
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


API_URL = 'https://api.developmentseed.org/satellites/'
S2_MGRS_GRID = os.path.join(os.path.dirname(os.path.abspath(__file__)), 's2_mgrs_grid.txt')


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

    # https://landsat.usgs.gov/what-are-landsat-collection-1-tiers
    return x


def bytes2str(x):
    """
    """
    return np.str(x.decode())


def parse_s2_tiling_grid(lon, lat, grid=S2_MGRS_GRID):
    """
    Search in the sentinel-2 tiling grid all the MGRS tiles containing a point.

    Args:
        lat, lon: geographic coordinates of the input point

    Returns:
        List of MGRS identifiers. Each MGRS identifier is a string of lenght 5.
        The first two characters indicate the utm code ranging from 01 to 60
        (indicating a longitude band), the next is an uppercase letter
        indicating the utm latitude band, and the last two are two uppercase
        letters giving a 100,000-meter square MGRS identifier.
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
    x = 'satellite_name=sentinel-2'

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
    url = '{}?search={}&limit=1000'.format(API_URL, search_string)

    # query Development Seed’s API
    r = requests.get(url)
    if r.ok:
        d = r.json()
    else:
        print('WARNING: request to {} returned {}'.format(url, r.status_code))
        return

    # for Landsat-8 keep only T1 and RT collection tiers:
    # https://landsat.usgs.gov/what-are-landsat-collection-1-tiers
    to_remove = set()
    if satellite == 'Landsat-8':
        for i, x in enumerate(d['results']):
            if x['COLLECTION_CATEGORY'] not in ['T1', 'RT']:
                to_remove.add(i)

    # check if the image footprint contains the area of interest
    aoi = shapely.geometry.shape(aoi)
    for i, x in enumerate(d['results']):
        if 'data_geometry' in x:
            if not shapely.geometry.shape(x['data_geometry']).contains(aoi):
                to_remove.add(i)

    for i in sorted(to_remove, reverse=True):  # delete the higher index first
        del d['results'][i]
        d['meta']['found'] -= 1

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
