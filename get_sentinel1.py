#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download and crop of Sentinel-1 images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>

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
import re
import glob
import zipfile
import datetime
import argparse
import subprocess

import bs4
import requests
import dateutil.parser
from osgeo import gdal
gdal.UseExceptions()

import srtm4
import utils


base_url = 'https://scihub.copernicus.eu/dhus'


def query_data_hub(output_filename, url, verbose=False, user='carlodef',
                   password='kayrros_cmla'):
    """
    Download a file from the Copernicus data hub.
    """
    verbosity = '--no-verbose' if verbose else '--quiet'  # more verbosity with --verbose
    subprocess.call(['wget',
                     verbosity,
                     '--no-check-certificate',
                     '--auth-no-challenge',
                     '--user=%s' % user,
                     '--password=%s' % password,
                     '--output-document=%s' % output_filename,
                     url])


def get_scihub_url(url, user='carlodef', password='kayrros_cmla'):
    """
    Request a scihub url.
    """
    r = requests.get(url, auth=(user, password))
    if r.ok:
        return r.text
    else:
        print('ERROR:', end=' ')
        if r.status_code == 503:
            print('The Sentinels Scientific Data Hub is down. Check on'
                  ' https://scihub.copernicus.eu/dhus/#/home')
        elif r.status_code == 401:
            print('Authentication failed with', user, password)
        else:
            print('Scientific Data Hub returned error', r.status_code)


def list_s1_images_scihub(lat, lon, w=None, h=None, start_date=None,
                          end_date=None, satellite='Sentinel-1',
                          product_type='GRD', operational_mode='IW'):
    """
    Return the list of Sentinel-1 products available on a given location.
    """
    # default start/end dates
    if start_date is None:
        start_date = datetime.datetime(2000, 1, 1)
    if end_date is None:
        end_date = datetime.datetime.now()

    # build the url used to query the scihub API
    query = 'platformname:{}'.format(satellite)
    query += ' AND producttype:{}'.format(product_type)
    query += ' AND sensoroperationalmode:{}'.format(operational_mode)
    query += ' AND beginposition:[{}Z TO {}Z]'.format(start_date.isoformat(),
                                                      end_date.isoformat())

    # queried polygon or point
    # http://forum.step.esa.int/t/advanced-search-in-data-hub-contains-intersects/1150/2
    if w is not None and h is not None:
        rectangle = utils.latlon_rectangle_centered_at(lat, lon, w, h)
        rectangle = [p[::-1] for p in rectangle]  # order is lon, lat for polygons
        poly_str = ', '.join(' '.join(str(x) for x in p) for p in rectangle)
        query += ' AND footprint:\"contains(POLYGON(({})))\"'.format(poly_str)
    else:
        # scihub ordering is lat, lon for points
        query += ' AND footprint:\"intersects({}, {})\"'.format(lat, lon)

    # send the query to scihub API
    url = '{}/search?q={}'.format(base_url, query)
    url += '&rows=100&start=0'  # otherwise we get only the first 10 results
    text = get_scihub_url(url)
    if text is None:
        return []

    # count the number of images
    images = bs4.BeautifulSoup(text, 'xml').find_all('entry')
    print('Found {} images'.format(len(images)))

    # loop over the images to extract uuids, filenames and acquisition dates
    out = []
    for img in images:

        # retrieve uuid
        uuid = img.find('id').text
        if uuid is None:
            for x in img.find_all('str'):
                if x.get('name') == 'uuid':
                    uuid = x.text

        # retrieve filename
        img_name = "%s.SAFE" % img.find('title').text
        if img_name is None:
            for x in img.find_all('str'):
                if x.get('name') == 'filename':
                    img_name = x.text

        # retrieve acquisition date
        for x in img.find_all('date'):
            if x.get('name') == 'beginposition':
                imaging_date = dateutil.parser.parse(x.text)

        out.append((uuid, img_name, imaging_date))

    return out


def download_and_crop_s1_images_scihub(images, lon, lat, w, h, out_dir='',
                                       cache_dir=''):
    """
    Download, extract and crop a list of Sentinel-1 images from the scihub.
    """
    # create output and cache directories
    if cache_dir:
        utils.mkdir_p(cache_dir)
    if out_dir:
        utils.mkdir_p(out_dir)

    # loop over the images to download, extract and crop
    for i in images:
        uuid, name, date = i
        filenames = glob.glob(os.path.join(cache_dir, name, 'measurement',
                                           's1a-iw-grd-vv-*.tif'))
        if not filenames or not utils.is_valid(filenames[0]):

            # download zip file from scihub
            zip_path = os.path.join(cache_dir, '{}.zip'.format(name))
            if not zipfile.is_zipfile(zip_path):
                url = "{}/odata/v1/Products('{}')/$value".format(base_url, uuid)
                query_data_hub(zip_path, url, verbose=True)

            # extract tiff image from the zip
            z = zipfile.ZipFile(zip_path, 'r')
            l = z.namelist()
            filenames = [
                x for x in l if 'measurement' in x and 'iw-grd-vv' in x]
            z.extract(filenames[0], path=cache_dir)

        # do the crop
        img = os.path.join(cache_dir, filenames[0])
        cx, cy = latlon_to_pix(img, lat, lon)
        x = cx - int(w / 2)
        y = cy - int(h / 2)

        crop = os.path.join(out_dir, date.strftime('%Y-%m-%dT%H:%M:%S.tif'))
        subprocess.call(['gdal_translate', img, crop, '-ot', 'UInt16',
                         '-srcwin', str(x), str(y), str(w), str(h)])


def latlon_to_pix(img_file, lat, lon):
    """
    Convert lat, lon coordinates to pixel coordinates.
    """
    # first get the altitude at the provided location from srtm
    alt = srtm4.srtm4(lon, lat)

    # then use gdaltransform to get pixel coordinates
    try:
        with open(img_file):
            p1 = subprocess.Popen(['echo', str(lon), str(lat), str(alt)],
                                  stdout=subprocess.PIPE)
            p2 = subprocess.Popen(['gdaltransform', '-tps', '-i', img_file],
                                  stdin=p1.stdout, stdout=subprocess.PIPE)
            line = p2.stdout.readlines()[-1]  # keep only the last line to discard warnings
            out = list(map(float, re.findall(b"\d+\.\d+", line)))
            col = int(round(out[0]))
            row = int(round(out[1]))
    except IOError:
        print("ERROR: file {} not found".format(img_file))
        return

    # check that the computed pixel is inside the image
    img = gdal.Open(img_file)
    if col < 0 or col > img.RasterXSize or row < 0 or row > img.RasterYSize:
        print("WARNING: point {} {} lies outside of image {}".format(lat, lon,
                                                                     img_file))
    return col, row


def get_time_series(lat, lon, w, h, start_date=None, end_date=None, out_dir='',
                    cache_dir=''):
    """
    Main function: download, crop and register a Sentinel-1 image time serie.
    """
    # list available images
    images = list_s1_images_scihub(lat, lon, w, h, start_date, end_date)

    # download and crop
    download_and_crop_s1_images_scihub(images, lon, lat, w, h, out_dir, cache_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Sentinel-1 images'))
    parser.add_argument('--lat', type=utils.valid_lat, required=True,
                        help=('latitude'))
    parser.add_argument('--lon', type=utils.valid_lon, required=True,
                        help=('longitude'))
    parser.add_argument('-s', '--start-date', type=utils.valid_datetime,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_datetime,
                        help='end date, YYYY-MM-DD')
    parser.add_argument('-w', '--width', type=int, help='width of the crop in pixels',
                        default=500)
    parser.add_argument('-l', '--height', type=int, help='height of the crop in pixels',
                        default=500)
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('--cache', type=str, help=('cache directory'),
                        default=os.path.abspath('.s1-cache'))
    args = parser.parse_args()

    get_time_series(args.lat, args.lon, args.width, args.height,
                    start_date=args.start_date, end_date=args.end_date,
                    out_dir=args.outdir, cache_dir=args.cache)
