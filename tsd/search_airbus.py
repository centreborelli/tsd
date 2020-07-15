#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Airbus archive images using Airbus API.

Copyright (C) 2018, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>

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

import os
import json
import argparse
import datetime
import requests
import shapely.geometry

from tsd import utils

API_URL = "https://search.federated.geoapi-airbusds.com/api/v1/search"


def satellite_to_constellation(s):
    if s in ['PHR1A', 'PHR1B']:
        return 'Pleiades'
    if s in ['SPOT5', 'SPOT6', 'SPOT7']:
        return 'SPOT'


def search(aoi, start_date=None, end_date=None, satellites=['PHR1A', 'PHR1B'], max_cloud_cover=10):
    """
    List images covering an area of interest (AOI) using Airbus API.

    Args:
        aoi: geojson.Polygon or geojson.Point object
        satellites (list of str): list of satellites names such as "PHR1A", "SPOT5", ...
        max_cloud_cover (float): max percentage of clouds covering the image
    """
    # date range
    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)

    # constellation
    constellations = list(set([satellite_to_constellation(s) for s in satellites]))

    # build the query
    query = {
        "geometry": shapely.geometry.shape(aoi).wkt,
        "constellation": constellations,
        "acquisitionDate": "[{},{}T23:59:59]".format(start_date.isoformat(),
                                                     end_date.isoformat()),
        #"incidenceAngle": "20]",
        "cloudCover": "{}]".format(max_cloud_cover),
        "count": 1000,
        "startPage": 1,
        "sortKeys": "acquisitionDate"
    }

    # query Airbus API
    headers = {
        'Authorization': 'Apikey {}'.format(os.environ['AIRBUS_DS_API']),
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
    }
    r = requests.post(API_URL, headers=headers, data=json.dumps(query))
    if r.ok:
        d = r.json()
    else:
        print('WARNING: request returned {}'.format(r.status_code))
        return

    # remove images that are from a different constellation
    to_remove = set()
    for i, x in enumerate(d['features']):
        if x['properties']['satellite'] not in satellites:
            print(x['properties']['satellite'])
            to_remove.add(i)

    # TODO: remove duplicated entries

    # check if the image footprint contains the area of interest
    aoi = shapely.geometry.shape(aoi)
    for i, x in enumerate(d['features']):
        if 'data_geometry' in x:
            if not shapely.geometry.shape(x['data_geometry']).contains(aoi):
                to_remove.add(i)

    for i in sorted(to_remove, reverse=True):  # delete the higher index first
        del d['features'][i]
        d['totalResults'] -= 1

    return d


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search of Airbus images.')
    parser.add_argument('--satellites', choices=['PHR1A', 'PHR1B'], nargs='*',
                        help=('either "PLEIADES" or "Spot"'),
                        default=['PHR1A', 'PHR1B'])
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
    parser.add_argument('-s', '--start-date', type=utils.valid_date,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_date,
                        help='end date, YYYY-MM-DD')
    parser.add_argument('--max-cloud', type=float, default=10,
                        help='maximum cloud cover (in percentage)')
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
                            end_date=args.end_date, satellites=args.satellites,
                            max_cloud_cover=args.max_cloud)))
