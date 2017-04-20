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

import bs4
import requests

import utils
import search_sentinel


scihub_url = 'https://scihub.copernicus.eu/dhus'
peps_url_search = 'https://peps.cnes.fr/resto/api/collections'
peps_url_download = 'https://peps.cnes.fr/resto/collections'


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


def download_sentinel_image(image, out_dir='', mirror='peps'):
    """
    Download a Sentinel image.
    """
    # create output directory
    if out_dir:
        utils.mkdir_p(out_dir)

    # download zip file
    zip_path = os.path.join(out_dir, '{}.SAFE.zip'.format(image['title']))
    if not zipfile.is_zipfile(zip_path) or os.stat(zip_path).st_size == 0:
        if mirror == 'scihub':
            url = "{}/odata/v1/Products('{}')/$value".format(scihub_url, image['id'])
            query_data_hub(zip_path, url, verbose=True)
        elif mirror == 'peps':
            r = requests.get('{}/S1/search.atom?identifier={}'.format(peps_url_search, image['title']))
            if r.ok:
                img = bs4.BeautifulSoup(r.text, 'xml').find_all('entry')[0]
                peps_id = img.find('id').text
                url = "{}/S1/{}/download".format(peps_url_download, peps_id)
                print("curl -k --basic -u carlodef@gmail.com:kayrros_cmla {} -o {}".format(url, zip_path))
                os.system("curl -k --basic -u carlodef@gmail.com:kayrros_cmla {} -o {}".format(url, zip_path))
            else:
                print('WARNING: failed request to {}/S1/search.atom?identifier={}'.format(peps_url_search, image['title']))
                print('WARNING: will download from scihub mirror...')
                download_sentinel_image(image, out_dir, mirror='scihub')
        else:
            print('ERROR: unknown mirror {}'.format(mirror))

    return zip_path


def get_time_series(lat, lon, w, h, start_date=None, end_date=None, out_dir='',
                    product_type='GRD', mirror='peps'):
    """
    Main function: download a Sentinel-1 image time serie.
    """
    # list available images
    images = search_sentinel.search_scihub(lat, lon, w, h, start_date,
                                           end_date, product_type=product_type)

    # download
    zips = []
    for image in images:
        zips.append(download_sentinel_image(image, out_dir, mirror))

    # unzip
    for z in zips:
        zipfile.ZipFile(z, 'r').extractall(path=out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Sentinel-1 images'))
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
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-t', '--product-type',
                        help='type of image: GRD or SLC', default='GRD')
    parser.add_argument('--mirror', help='download mirror: peps or scihub',
                        default='peps')
    args = parser.parse_args()

    get_time_series(args.lat, args.lon, args.width, args.height,
                    start_date=args.start_date, end_date=args.end_date,
                    out_dir=args.outdir, product_type=args.product_type, mirror=args.mirror)
