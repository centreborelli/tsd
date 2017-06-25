#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download of Sentinel-1 images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>
"""
from __future__ import print_function
import os
import zipfile
import argparse
import subprocess
import dateutil.parser

import bs4
import requests

import utils
import search_scihub


scihub_url = 'https://scihub.copernicus.eu/dhus'
peps_url_search = 'https://peps.cnes.fr/resto/api/collections'
peps_url_download = 'https://peps.cnes.fr/resto/collections'
codede_url = 'https://code-de.org/Sentinel1'


def query_data_hub(output_filename, url, verbose=False, user='carlodef',
                   password='kayrros_cmla'):
    """
    Download a file from the Copernicus data hub.
    """
    verbosity = '--verbose' if verbose else '--no-verbose'  # intermediate verbosity with --quiet
    subprocess.call(['wget',
                     verbosity,
                     '--no-check-certificate',
                     '--auth-no-challenge',
                     '--user=%s' % user,
                     '--password=%s' % password,
                     '--output-document=%s' % output_filename,
                     url])


def download_sentinel_image(image, out_dir='', mirror='code-de'):
    """
    Download a Sentinel image.
    """
    # create output directory
    if out_dir:
        utils.mkdir_p(out_dir)

    # download zip file
    zip_path = os.path.join(out_dir, '{}.SAFE.zip'.format(image['title']))
    date = [x['content'] for x in image['date'] if x['name'] == 'beginposition']
    date = dateutil.parser.parse(date[0])
    if not zipfile.is_zipfile(zip_path) or os.stat(zip_path).st_size == 0:
        if mirror == 'code-de':
            url = '{}/{:04d}/{:02d}/{:02d}/{}.SAFE.zip'.format(codede_url,
                                                               date.year,
                                                               date.month,
                                                               date.day,
                                                               image['title'])
            if requests.head(url).ok:  # download the file
                subprocess.call(['wget', url])
            else:  # switch to PEPS
                print('WARNING: {} not available, trying from PEPS...'.format(url))
                download_sentinel_image(image, out_dir, mirror='peps')
        elif mirror == 'peps':
            r = requests.get('{}/S1/search.atom?identifier={}'.format(peps_url_search, image['title']))
            try:
                img = bs4.BeautifulSoup(r.text, 'xml').find_all('entry')[0]
                peps_id = img.find('id').text
                url = "{}/S1/{}/download".format(peps_url_download, peps_id)
                print("curl -k --basic -u carlodef@gmail.com:kayrros_cmla {} -o {}".format(url, zip_path))
                os.system("curl -k --basic -u carlodef@gmail.com:kayrros_cmla {} -o {}".format(url, zip_path))
            except Exception:
                print('WARNING: failed request to {}/S1/search.atom?identifier={}'.format(peps_url_search, image['title']))
                print('WARNING: will download from scihub mirror...')
                download_sentinel_image(image, out_dir, mirror='scihub')
        elif mirror == 'scihub':
            url = "{}/odata/v1/Products('{}')/$value".format(scihub_url, image['id'])
            query_data_hub(zip_path, url, verbose=True)
        else:
            print('ERROR: unknown mirror {}'.format(mirror))

    return zip_path


def get_time_series(aoi, start_date=None, end_date=None, out_dir='',
                    product_type='GRD', mirror='code-de'):
    """
    Main function: download a Sentinel-1 image time serie.
    """
    # list available images
    images = search_scihub.search(aoi, start_date, end_date,
                                  product_type=product_type)

    # download
    zips = []
    for image in images:
        zips.append(download_sentinel_image(image, out_dir, mirror))

    # unzip
    for z in zips:
        zipfile.ZipFile(z, 'r').extractall(path=out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download '
                                                  'of Sentinel-1 images'))
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
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-t', '--product-type',
                        help='type of image: GRD, SLC, RAW', default='GRD')
    parser.add_argument('--mirror', help='download mirror: code-de, peps or scihub',
                        default='code-de')
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
    get_time_series(aoi, start_date=args.start_date, end_date=args.end_date,
                    out_dir=args.outdir, product_type=args.product_type,
                    mirror=args.mirror)
