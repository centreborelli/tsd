#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Planet images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

from __future__ import print_function
import argparse
import datetime
import json
import sys
import shapely.geometry
from planet import api

import utils


client = api.ClientV1()
ITEM_TYPES = ['PSScene4Band', 'PSScene3Band', 'PSOrthoTile', 'REScene', 'REOrthoTile',
              'Sentinel2L1C', 'Landsat8L1G']


def search(aoi, start_date=None, end_date=None, item_types=ITEM_TYPES):
    """
    Search for images using Planet API.

    Args:
        aoi: geojson.Polygon or geojson.Point object
        item_types: list of strings.
    """
    # default start/end dates
    if end_date is None:
        end_date = datetime.datetime.now()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)

    # build a search request with filters for the AOI and the date range
    geom_filter = api.filters.geom_filter(aoi)
    date_filter = api.filters.date_range('acquired', gte=start_date, lte=end_date)
    query = api.filters.and_filter(geom_filter, date_filter)
    request = api.filters.build_search_request(query, item_types)

    # this will cause an exception if there are any API related errors
    results = client.quick_search(request).get()

    # check if the image footprint contains the AOI
    aoi = shapely.geometry.shape(aoi)
    not_covering = []
    for x in results['features']:
        if not shapely.geometry.shape(x['geometry']).contains(aoi):
            not_covering.append(x)

    for x in not_covering:
        results['features'].remove(x)
    print('search_planet: removed {} results not covering the aoi'.format(len(not_covering)),
          file=sys.stderr)

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search of images through Planet API.')
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
    parser.add_argument('--item-types', nargs='*', default=ITEM_TYPES)
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
                            end_date=args.end_date,
                            item_types=args.item_types)))
