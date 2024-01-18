#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search through Copernicus data using Copernicus Data Space Ecosystem (CDSE) API.

Copyright (C) 2016-24, Carlo de Franchis <carlo.de-franchis@ens-paris-saclay.fr>

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
import json
import shapely.geometry
import requests
import urllib

from tsd import utils


# http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com

# CDSE OData API endpoint URL
API_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"


def build_odata_filter(aoi=None, start_date=None, end_date=None, satellite=None, product_type=None,
                       operational_mode=None, relative_orbit_number=None, orbit_direction=None,
                       swath_identifier=None, tile_id=None, title=None, tml=None, max_cloud_cover=None):
    """
    Args:
        aoi (dict): geometry formatted as a geojson polygon dict
        start_date (datetime):
        end_date (datetime):
        satellite (str): either "SENTINEL-1", "SENTINEL-2" or any item from the
            list of supported constellations
            https://documentation.dataspace.copernicus.eu/APIs/OData.html#query-collection-of-products
    """
    # build a filter as described in
    # https://documentation.dataspace.copernicus.eu/APIs/OData.html#filter-option
    filters = []

    if aoi is not None:
        filters.append("OData.CSC.Intersects(area=geography'SRID=4326;{}')".format(shapely.geometry.shape(aoi).wkt))

    if start_date is not None:
        filters.append("ContentDate/Start gt {}".format(start_date.isoformat()))

    if end_date is not None:
        filters.append("ContentDate/Start lt {}".format(end_date.isoformat()))

    if satellite is not None:
        satellite = satellite.upper()
        filters.append("Collection/Name eq '{}'".format(satellite.upper()))

    if product_type is not None:
        #filters.append("Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{}')".format(product_type))
        filters.append("contains(Name, '{}')".format(product_type))

    if relative_orbit_number is not None:
        filters.append("Attributes/OData.CSC.IntegerAttribute/any(att:att/Name eq 'relativeOrbitNumber' and att/OData.CSC.IntegerAttribute/Value eq {})".format(relative_orbit_number))

    if orbit_direction is not None:
        filters.append("Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'orbitDirection' and att/OData.CSC.StringAttribute/Value eq '{}')".format(orbit_direction))

    if operational_mode is not None:
        filters.append("Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq '{}')".format(operational_mode))

    if tile_id is not None:
        filters.append("Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'tileId' and att/OData.CSC.StringAttribute/Value eq '{}')".format(tile_id))

    if swath_identifier is not None:
        filters.append("Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'swathIdentifier' and att/OData.CSC.StringAttribute/Value eq '{}')".format(swath_identifier))

    if title is not None:
        filters.append("Name eq '{}'".format(title))

    if tml is not None:
        filters.append("Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'timeliness' and att/OData.CSC.StringAttribute/Value eq '{}')".format(tml))

    if max_cloud_cover is not None:
        filters.append("Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {})".format(max_cloud_cover))

    return " and ".join(filters)


def build_odata_query_url(max_nb_items=1000, orderby="ContentDate/Start",
                          expand_attributes=True, expand_assets=True,
                          **kwargs):
    """
    """
    filters = build_odata_filter(**kwargs)
    filters = urllib.parse.quote(filters)

    query_url = f"{API_URL}?$filter={filters}"

    if orderby is not None:
        query_url += f"&$orderby={orderby}%20desc"

    if max_nb_items is not None:
        query_url += f"&$top={max_nb_items}"

    if expand_attributes:
        query_url += "&$expand=Attributes"

    if expand_assets:
        query_url += "&$expand=Assets"

    return query_url


def search(aoi=None, search_type="intersects", **kwargs):
    """
    List the items intersecting an AOI by querying the CDSE API.
    """
    query_url = build_odata_query_url(aoi=aoi, **kwargs)

    r = requests.get(query_url)

    if not r.ok:
        print('ERROR:', end=' ')
        if r.status_code == 503:
            print('The Copernicus Data Space Ecosystem is down. Check on'
                  ' https://dataspace.copernicus.eu/')
        else:
            print('Copernicus Data Space Ecosystem returned error', r.status_code)
        r.raise_for_status()

    results = r.json()['value']

    # prettify attributes and assets
    for item in results:

        attributes = {a['Name']: a['Value'] for a in item['Attributes']}
        item.pop('Attributes')
        item.update(attributes)

        assets = {a['Type']: a['DownloadLink'] for a in item['Assets']}
        item.pop('Assets')
        item.update(assets)

    # TODO
#     if aoi is not None and search_type == "contains":
#         # check if the image footprint contains the area of interest
#         not_covering = []
#         aoi_shape = shapely.geometry.shape(aoi)
#         for x in results:
#             if not shapely.wkt.loads(x['footprint']).contains(aoi_shape):
#                 not_covering.append(x)
#
#         for x in not_covering:
#             results.remove(x)

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Search in CDSE catalog'))
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
    parser.add_argument('--satellite',
                        help='which satellite: Sentinel-{1,2,3,5P,6} or Landsat-{5,7,8}')
    parser.add_argument('--product-type',
                        help='type of image: RAW, SLC, GRD, OCN (for S1), L1C, L2A (for S2)')
    parser.add_argument('--operational-mode',
                        help='(for S1) acquisiton mode: SM, IW, EW or WV')
    parser.add_argument('--swath-identifier',
                        help='(for S1) subswath id: S1..S6 or IW1..IW3 or EW1..EW5')
    parser.add_argument('--tile-id',
                        help='(for S2) MGRS tile identifier, e.g. 31TCJ')
    parser.add_argument('--title',
                        help='Product title (e.g. S2A_MSIL1C_20160105T143732_N0201_R096_T19KGT_20160105T143758)')
    parser.add_argument('--relative-orbit-number', type=int,
                        help='Relative orbit number, e.g. 98')
    args = parser.parse_args()

    if args.lat is not None and args.lon is not None:
        args.geom = utils.geojson_geometry_object(args.lat, args.lon,
                                                  args.width, args.height)

    print(json.dumps(search(aoi=args.geom,
                            start_date=args.start_date,
                            end_date=args.end_date,
                            satellite=args.satellite,
                            product_type=args.product_type,
                            operational_mode=args.operational_mode,
                            swath_identifier=args.swath_identifier,
                            relative_orbit_number=args.relative_orbit_number,
                            tile_id=args.tile_id,
                            title=args.title)))
