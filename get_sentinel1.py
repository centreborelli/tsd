#!/usr/bin/env python3
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


PEPS_URL_SEARCH = 'https://peps.cnes.fr/resto/api/collections'
PEPS_URL_DOWNLOAD = 'https://peps.cnes.fr/resto/collections'
CODEDE_URL = 'https://code-de.org/Sentinel1'


def query_data_hub(output_filename, url, verbose=False,
                   user=search_scihub.login,
                   password=search_scihub.password):
    """
    Download a file from the Copernicus data hub or one of its true mirrors.
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


def download_safe_from_peps(safe_name, out_dir=''):
    """
    """
    try:
        login, password = os.environ['PEPS_LOGIN'], os.environ['PEPS_PASSWORD']
    except KeyError:
        print("Downloading from PEPS requires the PEPS_LOGIN and".format(__file__),
              "PEPS_PASSWORD environment variables to be defined with valid",
              "credentials for https://peps.cnes.fr/. Create an account if",
              "you don't have one (it's free) then edit the relevant configuration",
              "files (eg .bashrc) to define these environment variables.")
        return

    query = '{}/S1/search.atom?identifier={}'.format(PEPS_URL_SEARCH, safe_name)
    r = requests.get(query)

    if not r.ok:
        print('WARNING: request {} failed'.format(query))
        return

    img = bs4.BeautifulSoup(r.text, 'xml').find_all('entry')[0]
    peps_id = img.find('id').text
    url = "{}/S1/{}/download".format(PEPS_URL_DOWNLOAD, peps_id)
    zip_path = os.path.join(out_dir, '{}.SAFE.zip'.format(safe_name))
    cmd = "curl -k --basic -u {}:{} {} -o {}".format(login, password,
                                                     url, zip_path)
    print(cmd)
    os.system(cmd)


def download_sentinel_image(image, out_dir='', mirror='peps'):
    """
    Download a Sentinel image.
    """
    # create output directory
    if out_dir:
        utils.mkdir_p(out_dir)

    # download zip file
    zip_path = os.path.join(out_dir, '{}.SAFE.zip'.format(image['title']))
    date = dateutil.parser.parse(image['beginposition'], ignoretz=True)
    if not zipfile.is_zipfile(zip_path) or os.stat(zip_path).st_size == 0:
        if mirror == 'code-de':
            url = '{}/{:04d}/{:02d}/{:02d}/{}.SAFE.zip'.format(CODEDE_URL,
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
            try:
                download_safe_from_peps(image['title'], out_dir=out_dir)
            except Exception:
                print('WARNING: failed request to {}/S1/search.atom?identifier={}'.format(PEPS_URL_SEARCH, image['title']))
                print('WARNING: will download from copernicus mirror...')
                download_sentinel_image(image, out_dir, mirror='copernicus')
        elif mirror in search_scihub.API_URLS:
            url = "{}/odata/v1/Products('{}')/$value".format(search_scihub.API_URLS[mirror],
                                                             image['id'])
            query_data_hub(zip_path, url, verbose=False)
        else:
            print('ERROR: unknown mirror {}'.format(mirror))

    return zip_path


def get_time_series(aoi, start_date=None, end_date=None, out_dir='',
                    product_type='GRD', operational_mode='IW',
                    search_api='copernicus', download_mirror='peps'):
    """
    Main function: download a Sentinel-1 image time serie.
    """
    # list available images
    images = search_scihub.search(aoi, start_date, end_date,
                                  product_type=product_type,
                                  operational_mode=operational_mode,
                                  api=search_api)

    # download
    for image in images:
        download_sentinel_image(image, out_dir, download_mirror)


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
    parser.add_argument('--title', help='image title (filename.SAFE)')
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-t', '--product-type',
                        help='type of image: GRD, SLC, RAW', default='GRD')
    parser.add_argument('-m', '--operational-mode', default='IW',
                        help='acquisiton mode: SM, IW, EW or WV')
    parser.add_argument('--api', default='copernicus',
                        help='search API to use: copernicus, austria or finland')
    parser.add_argument('--mirror', default='peps',
                        help='download mirror: peps, copernicus, austria or finland')
    args = parser.parse_args()

    if args.geom and (args.lat or args.lon):
        parser.error('--geom and {--lat, --lon} are mutually exclusive')

    if args.title and (args.geom or args.lat or args.lon):
        parser.error('--title, --geom and {--lat, --lon} are mutually exclusive')

    if not args.geom and (not args.lat or not args.lon) and (not args.title):
        parser.error('either --geom, {--lat, --lon} or --title must be defined')

    if args.title:
        download_safe_from_peps(args.title, args.outdir)
    else:
        if args.geom:
            aoi = args.geom
        else:
            aoi = utils.geojson_geometry_object(args.lat, args.lon, args.width,
                                                args.height)
        get_time_series(aoi, start_date=args.start_date, end_date=args.end_date,
                        out_dir=args.outdir, product_type=args.product_type,
                        operational_mode=args.operational_mode,
                        search_api=args.api, download_mirror=args.mirror)
