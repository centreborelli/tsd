#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Sentinel images.

Copyright (C) 2016-18, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>

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
import sys
import argparse
import datetime
import json
import shapely.geometry
import shapely.wkt
import requests
import dateutil.parser

from tsd import utils


# http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com
API_URLS = {
    'copernicus': 'https://scihub.copernicus.eu/dhus/',
    'austria': 'https://data.sentinel.zamg.ac.at/',
    'finland': 'https://finhub.nsdc.fmi.fi/'
}


def read_copernicus_credentials_from_environment_variables():
    """
    Read the user Copernicus Open Access Hub credentials.
    """
    try:
        login = os.environ['COPERNICUS_LOGIN']
        password = os.environ['COPERNICUS_PASSWORD']
    except KeyError as e:
        login = 'tsd-default-user'
        password = 'b3c5e714034282ea5c'
    return login, password


def post_scihub(url, query, user, password):
    """
    Send a POST request to scihub.
    """
    r = requests.post(url, dict(q=query), auth=(user, password))
    if r.ok:
        return r
    else:
        print('ERROR:', end=' ')
        if r.status_code == 503:
            print('The Sentinels Scientific Data Hub is down. Check on'
                  ' https://scihub.copernicus.eu/dhus/#/home')
        elif r.status_code == 401:
            print('Authentication failed with', user, password)
        else:
            print('Scientific Data Hub returned error', r.status_code)
        r.raise_for_status()


def build_scihub_query(aoi, start_date=None, end_date=None,
                       satellite='Sentinel-1', product_type='GRD',
                       operational_mode='IW', relative_orbit_number=None,
                       swath_identifier=None):
    """
    """
    # default start/end dates
    if end_date is None:
        end_date = datetime.datetime.now()
    if start_date is None:
        start_date = datetime.datetime(2000, 1, 1)

    # build the url used to query the scihub API
    query = 'platformname:{}'.format(satellite)
    query += ' AND producttype:{}'.format(product_type)
    if satellite == 'Sentinel-1':
        query += ' AND sensoroperationalmode:{}'.format(operational_mode)
    query += ' AND beginposition:[{}Z TO {}Z]'.format(start_date.isoformat(),
                                                      end_date.isoformat())
    if relative_orbit_number is not None:
        query += ' AND relativeorbitnumber:{}'.format(relative_orbit_number)

    if swath_identifier is not None:
        query += ' AND swathidentifier:{}'.format(swath_identifier)

    # queried polygon or point
    # http://forum.step.esa.int/t/advanced-search-in-data-hub-contains-intersects/1150/2
    query += ' AND footprint:\"contains({})\"'.format(shapely.geometry.shape(aoi).wkt)

    return query


def load_query(query, api_url, start_row=0, page_size=100):
    """
    Do a full-text query on the SciHub API using the OpenSearch format.

    https://scihub.copernicus.eu/twiki/do/view/SciHubUserGuide/3FullTextSearch
    """
    # load query results
    url = '{}search?format=json&rows={}&start={}'.format(api_url, page_size,
                                                         start_row)
    login, password = read_copernicus_credentials_from_environment_variables()
    r = post_scihub(url, query, login, password)

    # parse response content
    d = r.json()['feed']
    total_results = int(d['opensearch:totalResults'])
    entries = d.get('entry', [])

    # if the query returns only one product entries will be a dict, not a list
    if isinstance(entries, dict):
        entries = [entries]

    # repeat query until all results have been loaded
    output = entries
    if total_results > start_row + page_size:
        output += load_query(query, api_url, start_row=(start_row + page_size))
    return output


def prettify_scihub_dict(d):
    """
    Convert the oddly formatted json response of scihub into something nicer.

    Scihub json response is roughly a dict with a key per datatype (int, str,
    date, ...). The value associated to each key is a list of dicts like
    {'name': foo, 'content': bar}, while it would be more natural to have
    (foo, bar) as a (key, value) pair of the main json dict.

    Args:
        d (dict): json-formatted metadata returned by scihub for a single SAFE

    Returns:
        prettified metadata dict
    """
    out = {}
    for k in ['title', 'id', 'summary']:
        if k in d:
            out[k] = d[k]

    if 'int' in d:
        for x in d['int']:
            out[x['name']] = int(x['content'])

    if 'str' in d:
        for x in d['str']:
            out[x['name']] = x['content']

    if 'date' in d:
        for x in d['date']:
            out[x['name']] = x['content']

    if 'link' in d:
        out['links'] = {}
        for x in d['link']:
            if 'rel' in x:
                out['links'][x['rel']] = x['href']
            else:
                out['links']['main'] = x['href']
    return out


def search(aoi, start_date=None, end_date=None, satellite='Sentinel-1',
           product_type='GRD', operational_mode='IW',
           relative_orbit_number=None, swath_identifier=None,
           api='copernicus'):
    """
    List the Sentinel images covering a location using Copernicus Scihub API.
    """
    if satellite == 'Sentinel-2' and product_type not in ['S2MSI1C', 'S2MSI2A', 'S2MSI2Ap']:
        product_type = 'S2MSI1C'

    query = build_scihub_query(aoi, start_date, end_date, satellite,
                               product_type, operational_mode,
                               relative_orbit_number, swath_identifier)
    results = [prettify_scihub_dict(x) for x in load_query(query, API_URLS[api])]

    # check if the image footprint contains the area of interest
    not_covering = []
    aoi_shape = shapely.geometry.shape(aoi)
    for x in results:
        if not shapely.wkt.loads(x['footprint']).contains(aoi_shape):
            not_covering.append(x)

    for x in not_covering:
        results.remove(x)

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Search of Sentinel images'))
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
    parser.add_argument('--satellite', default='Sentinel-1',
                        help='which satellite: Sentinel-1 or Sentinel-2')
    parser.add_argument('--product-type', default='GRD',
                        help='type of image: RAW, SLC, GRD, OCN (for S1), S2MSI1C, S2MSI2A, S2MSI2Ap (for S2)')
    parser.add_argument('--operational-mode', default='IW',
                        help='(for S1) acquisiton mode: SM, IW, EW or WV')
    parser.add_argument('--swath-identifier',
                        help='(for S1) subswath id: S1..S6 or IW1..IW3 or EW1..EW5')
    parser.add_argument('--api', default='copernicus',
                        help='mirror to use: copernicus, austria or finland')
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

    print(json.dumps(search(aoi, args.start_date, args.end_date,
                            satellite=args.satellite,
                            product_type=args.product_type,
                            operational_mode=args.operational_mode,
                            swath_identifier=args.swath_identifier,
                            api=args.api)))
