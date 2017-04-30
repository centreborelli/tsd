#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Search of Sentinel images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>
"""

from __future__ import print_function
import sys
import argparse
import datetime
import requests
import json
import shapely.geometry
import shapely.wkt
import dateutil
import re
import bs4
import utm
import numpy as np
import matplotlib

import utils


# http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com
aws_url = 'http://sentinel-s2-l1c.s3.amazonaws.com'
scihub_url = 'https://scihub.copernicus.eu/dhus/'


def post_scihub(url, query, user='carlodef', password='kayrros_cmla'):
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


def build_scihub_query(lat, lon, w=None, h=None, start_date=None,
                       end_date=None, satellite='Sentinel-1',
                       product_type='GRD', operational_mode='IW'):
    """
    """
    # default start/end dates
    if start_date is None:
        start_date = datetime.datetime(2000, 1, 1)
    if end_date is None:
        end_date = datetime.datetime.now()

    # build the url used to query the scihub API
    query = 'platformname:{}'.format(satellite)
    if satellite == 'Sentinel-1':
        query += ' AND producttype:{}'.format(product_type)
        query += ' AND sensoroperationalmode:{}'.format(operational_mode)
    query += ' AND beginposition:[{}Z TO {}Z]'.format(start_date.isoformat(),
                                                      end_date.isoformat())

    # queried polygon or point
    # http://forum.step.esa.int/t/advanced-search-in-data-hub-contains-intersects/1150/2
    if w is not None and h is not None:
        rectangle = utils.lonlat_rectangle_centered_at(lon, lat, w, h)
        poly_str = ', '.join(' '.join(str(x) for x in p) for p in rectangle)
        query += ' AND footprint:\"contains(POLYGON(({})))\"'.format(poly_str)
    else:
        # scihub ordering is lat, lon for points
        query += ' AND footprint:\"contains({}, {})\"'.format(lat, lon)

    return query


def load_query(query, start_row=0, page_size=100):
    """
    Do a full-text query on the SciHub API using the OpenSearch format.

    https://scihub.copernicus.eu/twiki/do/view/SciHubUserGuide/3FullTextSearch
    """
    # load query results
    url = '{}search?format=json&rows={}&start={}'.format(scihub_url, page_size,
                                                         start_row)
    r = post_scihub(url, query)

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
        output += load_query(query, start_row=(start_row + page_size))
    return output


def build_aws_url_from_scihub_dict(d):
    """
    """
    date_string = [a['content'] for a in d['date'] if a['name'] == 'beginposition'][0]
    date = dateutil.parser.parse(date_string)

    # we assume the product name is formatted as in:
    # S2A_MSIL1C_20170410T171301_N0204_R112_T14SQE_20170410T172128
    # the mgrs_id (here 14SQE) is read from the product name in '_T14SQE_'
    mgrs_id = re.findall(r"_T([0-9]{2}[A-Z]{3})_", d['title'])[0]
    utm_code, lat_band, sqid = mgrs_id[:2], mgrs_id[2], mgrs_id[3:]

    return '{}/tiles/{}/{}/{}/{}/{}/{}/0/'.format(aws_url, utm_code, lat_band,
                                                  sqid, date.year, date.month,
                                                  date.day)


def is_image_cloudy_at_location(image, lat, lon, w=50):
    """
    Tell if the given location is covered by clouds in a given image (metadata).

    The location is considered covered if a cloud intersects the square of size
    w centered on the location.

    Args:
        image: dictionary returned by the Scihub API
        lat, lon: geographic coordinates of the input location
        w: width in meters of a square centred around (lat, lon)
    """
    polygons = []
    url = build_aws_url_from_scihub_dict(image)
    url = requests.compat.urljoin(url, 'qi/MSK_CLOUDS_B00.gml')
    r = requests.get(url)
    if r.ok:
        soup = bs4.BeautifulSoup(r.text, 'xml')
        for polygon in soup.find_all('MaskFeature'):
            if polygon.maskType.text == 'OPAQUE':  # either OPAQUE or CIRRUS
                polygons.append(polygon)
    else:
        print("WARNING: couldn't retrieve cloud mask file", url)

    utm_x, utm_y = utm.from_latlon(lat, lon)[:2]
    for polygon in polygons:
        try:
            coords = list(map(float, polygon.posList.text.split()))
            points = list(zip(coords[::2], coords[1::2]))
            if polygon_intersects_bbox(points, [[utm_x - w, utm_y - w],
                                                [utm_x + w, utm_y + w]]):
                return True
        except IndexError:
            pass
    return False


def polygon_intersects_bbox(polygon, bbox):
    """
    Check if a polygon intersects a rectangle.

    Args:
        poly: list of 2D points defining a polygon. Each 2D point is represented
            by a pair of coordinates
        bbox: list of two opposite corners of the rectangle. Each corner is
            represented by a pair of coordinates

    Returns:
        True if the rectangle intersects the polygon
    """
    if np.array(polygon).ndim != 2:
        print('WARNING: wrong shape for polygon', polygon)
        return False
    else:
        p = matplotlib.path.Path(polygon)
        b = matplotlib.transforms.Bbox(bbox)
        return p.intersects_bbox(b)


def search(lat, lon, w=None, h=None, start_date=None, end_date=None,
           satellite='Sentinel-1', product_type='GRD', operational_mode='IW'):
    """
    List the Sentinel images covering a location using Copernicus Scihub API.
    """
    query = build_scihub_query(lat, lon, w, h, start_date, end_date, satellite,
                               product_type, operational_mode)
    results = load_query(query)
    print('Found {} images'.format(len(results)), file=sys.stderr)

    # check if the image footprint contains the point or region of interest (roi)
    if w is not None and h is not None:
        roi = shapely.geometry.Polygon(utils.lonlat_rectangle_centered_at(lon, lat, w, h))
    else:
        roi = shapely.geometry.Point(lon, lat)

    not_covering = []
    for x in results:
        footprint = [a['content'] for a in x['str'] if a['name'] == 'footprint'][0]
        if not shapely.wkt.loads(footprint).contains(roi):
            not_covering.append(x)

    for x in not_covering:
        results.remove(x)
    print('{} images containing the region of interest'.format(len(results)),
          file=sys.stderr)

    # remove images completely covered with clouds (for Sentinel-2 only)
    if  satellite == 'Sentinel-2':
        cloudy = [x for x in results if is_image_cloudy_at_location(x, lat, lon)]
        for x in cloudy:
            results.remove(x)
        print('{} non covered by clouds'.format(len(results)), file=sys.stderr)

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Search of Sentinel images'))
    parser.add_argument('--lat', type=utils.valid_lat, required=True,
                        help=('latitude'))
    parser.add_argument('--lon', type=utils.valid_lon, required=True,
                        help=('longitude'))
    parser.add_argument('-w', '--width', type=int, help='width of the area in meters',
                        default=1000)
    parser.add_argument('-l', '--height', type=int, help='height of the area in meters',
                        default=1000)
    parser.add_argument('-s', '--start-date', type=utils.valid_datetime,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_datetime,
                        help='end date, YYYY-MM-DD')
    parser.add_argument('--satellite', default='Sentinel-1',
                        help='which satellite: Sentinel-1 or Sentinel-2')
    parser.add_argument('--product-type', default='GRD',
                        help='(for S1) type of image: GRD or SLC')
    parser.add_argument('--operational-mode', default='IW',
                        help='(for S1) acquisiton mode: SM, IW, EW or WV')
    args = parser.parse_args()

    images = search(args.lat, args.lon, args.width, args.height,
                    args.start_date, args.end_date, satellite=args.satellite,
                    product_type=args.product_type,
                    operational_mode=args.operational_mode)
    print(json.dumps(images))
#    for image in images:
#        print(image['summary'])
