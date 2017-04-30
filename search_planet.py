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
import shapely.geometry
import geojson
from planet import api

import utils


client = api.ClientV1()
ITEM_TYPES = ['PSScene4Band', 'PSScene3Band', 'PSOrthoTile', 'REScene', 'REOrthoTile',
              'Sentinel2L1C', 'Landsat8L1G']


def search(lat, lon, w=None, h=None, start_date=None, end_date=None,
                  item_types=ITEM_TYPES):
    """
    Search for images using Planet API.

    Args:
        item_types: list of strings.
    """
    # area of interest (AOI)
    if w is not None and h is not None:  # rectangle
        aoi = geojson.Polygon([utils.lonlat_rectangle_centered_at(lon, lat, w, h)])
    else:  # point
        aoi = geojson.Point([lon, lat])
    
    # default start/end dates
    if start_date is None:
        start_date = datetime.datetime(2015, 8, 1)
    if end_date is None:
        end_date = datetime.datetime.now()

    # planet date range filter
    date_range_filter = {
      "type": "DateRangeFilter",
      "field_name": "acquired",
      "config": {
        "gte": start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        "lte": end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
      }
    }
    
    # build a filter for the AOI and the date range
    query = api.filters.and_filter(api.filters.geom_filter(aoi), date_range_filter)
    
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
    #print('removed {}'.format(len(not_covering)))

    return results 


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search of images through Planet API.')
    parser.add_argument('--lat', type=utils.valid_lat, required=True,
                        help=('latitude of the interest point'))
    parser.add_argument('--lon', type=utils.valid_lon, required=True,
                        help=('longitude of the interest point'))
    parser.add_argument('-w', '--width', type=int, help='width of the area (m)')
    parser.add_argument('-l', '--height', type=int, help='height of the area (m)')
    parser.add_argument('-s', '--start-date', type=utils.valid_datetime,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_datetime,
                        help='end date, YYYY-MM-DD')
    args = parser.parse_args()

    print(json.dumps(search(args.lat, args.lon, args.width, args.height,
                            start_date=args.start_date,
                            end_date=args.end_date)))
