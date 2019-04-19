#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Planet images.

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
import sys
import shapely.geometry
import dateutil.parser
from planet import api

from tsd import utils

client = api.ClientV1()
ITEM_TYPES = ['PSScene3Band', 'PSScene4Band', 'PSOrthoTile', 'REScene', 'REOrthoTile',
              'Sentinel2L1C', 'Landsat8L1G', 'Sentinel1', 'SkySatScene']


def search(aoi, start_date=None, end_date=None, item_types=ITEM_TYPES,
           satellite_id=None, search_type='contains', remove_duplicates=True):
    """
    Search for images using Planet API.

    Args:
        aoi: geojson.Polygon or geojson.Point object
        item_types: list of strings.
        satellite_id (str): satellite identifier, e.g. '0f02'
        search_type (str): either 'intersects' or 'contains'

    """
    # default start/end dates
    if end_date is None:
        end_date = datetime.datetime.now()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)

    # build a search query with filters for the AOI and the date range
    geom_filter = api.filters.geom_filter(aoi)
    date_filter = api.filters.date_range('acquired', gte=start_date, lte=end_date)
    if 'PSScene3Band' in item_types or 'PSScene4Band' in item_types:
        quality_filter = api.filters.string_filter('quality_category', 'standard')
        query = api.filters.and_filter(geom_filter, date_filter, quality_filter)
    else:
        query = api.filters.and_filter(geom_filter, date_filter)

    if satellite_id:
        query = api.filters.and_filter(query,
                                       api.filters.string_filter('satellite_id',
                                                                 satellite_id))

    request = api.filters.build_search_request(query, item_types)

    # this will cause an exception if there are any API related errors
    try:
        response = client.quick_search(request)
    except api.exceptions.InvalidAPIKey as e:
        print("\nERROR: The {} module requires".format(os.path.basename(__file__)),
              "the PL_API_KEY environment variable to be defined with valid",
              "credentials for https://www.planet.com/. Create an account if",
              "you don't have one (it's free) then edit the relevant configuration",
              "files (eg .bashrc) to define this environment variable.\n")
        raise e

    # list results
    aoi = shapely.geometry.shape(aoi)
    results = []
    for x in response.items_iter(limit=None):
        if search_type == 'contains':  # keep only images containing the full AOI
            if not shapely.geometry.shape(x['geometry']).contains(aoi):
                continue
        results.append(x)

    # sort results by acquisition date
    dates = [dateutil.parser.parse(x['properties']['acquired']) for x in results]
    results = [r for d, r in sorted(zip(dates, results), key=lambda t:t[0])]
    dates.sort()

    # remove duplicates (two images are said to be duplicates if within 5 minutes)
    if remove_duplicates:
        to_remove = []
        for i, (d, r) in enumerate(list(zip(dates, results))[:-1]):
            if dates[i+1] - d < datetime.timedelta(seconds=300):
                to_remove.append(r)

    return [r for r in results if r not in to_remove]


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
    parser.add_argument('--search-type', choices=['contains', 'intersects'],
                        default='contains', help='search type')
    parser.add_argument('--satellite-id', help='satellite identifier, e.g. 0f02')
    parser.add_argument('--keep-duplicates', action='store_true',
                        help='keep all images even when two were acquired within'
                             ' less than 5 minutes (the default behaviour is to'
                             ' discard such duplicates)')
    parser.add_argument('--item-types', nargs='*', choices=ITEM_TYPES,
                        default=['PSScene3Band'], metavar='',
                        help=('space separated list of item types to'
                              ' search for. Default is PSScene3Band. Allowed'
                              ' values are {}'.format(', '.join(ITEM_TYPES))))
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
                            item_types=args.item_types,
                            search_type=args.search_type,
                            satellite_id=args.satellite_id,
                            remove_duplicates=~args.keep_duplicates)))
