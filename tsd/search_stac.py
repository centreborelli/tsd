#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Landsat-8 and Sentinel-2 images using STAC APIs.

Copyright (C) 2016-20, Carlo de Franchis <carlo.de-franchis@ens-paris-saclay.fr>

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

import argparse
import datetime
import json
import shapely.geometry

import satsearch

from tsd import utils


ENDPOINT = "https://sat-api.developmentseed.org"  # implements STAC 0.6.0, requires sat-search 0.2.x
ENDPOINT = "https://earth-search.aws.element84.com/v0"  # implements STAC 1.0.0, requires sat-search 0.3.x


def search(aoi, start_date=None, end_date=None, satellite="sentinel-2", product_type=None):
    """
    List images covering an area of interest (AOI) using a STAC compliant API.

    Args:
        aoi (geojson.Polygon): area of interest
        satellite (str): either Landsat-8 or Sentinel-2
        product_type (str, optional): for Sentinel-2, either "L1C" or "L2A".
            Ignored for Landsat.
    """
    # date range
    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)

    # collection
    if satellite.lower() in ["sentinel-2", "sentinel2", "sentinel"]:
        collections = ["sentinel-s2-l1c", "sentinel-s2-l2a-cogs"]
        if product_type is not None:
            if product_type.lower() in ["1c", "l1c"]:
                collections = ["sentinel-s2-l1c"]
            elif product_type.lower() in ["2a", "l2a"]:
                collections = ["sentinel-s2-l2a-cogs"]
            else:
                raise TypeError(f"unknown product_type {product_type}")
    elif satellite.lower() in ["landsat-8", "landsat8", "landsat"]:
        collections = ["landsat-8-l1-c1"]
    else:
        raise TypeError(('Satellite "{}" not supported. Use either Landsat-8 or'
                         ' Sentinel-2.').format(satellite))

    # query the STAC compliant API
    r = satsearch.Search(url=ENDPOINT,
                         intersects=aoi,
                         datetime='{}/{}'.format(start_date.isoformat(),
                                                 end_date.isoformat()),
                         collections=collections)

    # check if the images footprints contain the area of interest
    aoi = shapely.geometry.shape(aoi)
    results = []
    for x in r.items():
        try:
            if shapely.geometry.shape(x.geometry).contains(aoi):
                results.append(vars(x)['_data'])
        except AttributeError:
            pass

    return results


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
