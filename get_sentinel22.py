#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic crop and download of Sentinel-2 images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

from __future__ import print_function
import re
import os
import sys
import shutil
import argparse
import multiprocessing
import dateutil.parser
import datetime
import requests
import bs4
import boto3
import botocore
import geojson
import shapely.geometry
from google.cloud import storage

import utils
import parallel
import metadata_parser

# list of spectral bands
ALL_BANDS = ['TCI', 'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08',
             'B8A', 'B09', 'B10', 'B11', 'B12']

def check_args(search_api, mirror, product_type):
    if product_type is not None and search_api!='scihub':
        print("WARNING: product_type option is available only with search_api='scihub'")
    if mirror=='gcloud' and search_api not in ['gcloud', 'devseed']:
        raise ValueError("ERROR: You must use gcloud or devseed search_api to use gcloud as mirror")
    if search_api=='gcloud' and mirror!='gcloud':
        raise ValueError("ERROR: You must use gcloud mirror to use gcloud as search_api")
    if search_api=='gcloud':
        try:
            private_key = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        except KeyError:
            raise ValueError('You must have the env variable GOOGLE_APPLICATION_CREDENTIALS linking to the cred json file')
    if mirror=='aws':
        info_url = "https://forum.sentinel-hub.com/t/changes-of-the-access-rights-to-l1c-bucket-at-aws-public-datasets-requester-pays/172"

        if 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
            try:
                boto3.session.Session().client('s3').list_objects_v2(Bucket=metadata_parser.AWS_S3_URL_L1C[5:],
                                                                     RequestPayer='requester')
            except botocore.exceptions.ClientError:
                raise ValueError('Could not connect to AWS server. Check credentials or use mirror=gcloud')
        else:
            raise ValueError("TSD downloads Sentinel-2 image crops from the s3://sentinel-s2-l1c",
                              "AWS bucket, which used to be free. On the 7th of August 2018,",
                              "the bucket was switched to 'Requester Pays'. As a consequence,",
                              "you need an AWS account and your credentials stored in the",
                              "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment",
                              "variables in order to use TSD. The price ranges in 0.05-0.09 $",
                              "per GB. More info: {}".format(info_url))


def global_search(search_api, aoi, start_date, end_date, product_type):
    if search_api == 'devseed':
        import search_devseed
        images = search_devseed.search(aoi, start_date, end_date,'Sentinel-2')['results']
        images = [metadata_parser.DevSeedParser(img) for img in images]
    elif search_api == 'scihub':
        import search_scihub
        if product_type is not None:
            product_type = 'S2MSI{}'.format(product_type[1:])
        images = search_scihub.search(aoi, start_date, end_date,
                                      satellite='Sentinel-2',
                                      product_type=product_type)
        images = [metadata_parser.SciHubParser(img) for img in images]
    elif search_api == 'planet':
        import search_planet
        images = search_planet.search(aoi, start_date, end_date,
                                      item_types=['Sentinel2L1C'])
        images = [metadata_parser.PlanetParser(img) for img in images]
    elif search_api == 'gcloud':
        import search_gcloud
        images = search_gcloud.search(aoi, start_date, end_date)
        images = [metadata_parser.GcloudParser(img) for img in images]

    images.sort(key=(lambda k: (k.date, k.mgrs_id)))
    return images


def download(aoi, bands, imgs, mirror, out_dir, parallel_downloads):
    seen = set()
    imgs = [img for img in imgs if not img.date in seen or seen.add(img.date)]
    crops_args = []
    for img in imgs:
        coords = utils.utm_bbx(aoi,  # convert aoi coordates to utm
                               utm_zone=int(img.utm_zone),
                               r=60)  # round to multiples of 60 (B01 resolution)
        for b in bands:
            fname = os.path.join(out_dir, '{}_band_{}.tif'.format(img.filename, b))
            crops_args.append((fname, img.urls[mirror][b], *coords))

    utils.mkdir_p(out_dir)
    print('Downloading {} crops ...'.format(len(crops_args), end=' '))
    parallel.run_calls(utils.crop_with_gdal_translate, crops_args,
                       extra_args=('UInt16',), pool_type='threads',
                       nb_workers=parallel_downloads)
    return crops_args

def bands_files_are_valid(img, bands, search_api, directory):
    """
    Check if all bands images files are valid.
    """
    name = img.filename
    filenames = ['{}_band_{}.tif'.format(name, b) for b in bands]
    paths = [os.path.join(directory, f) for f in filenames]
    return all(utils.is_valid(p) for p in paths)


def is_img_cloudy(img, aoi, mirror, p=0.5):
    url = img.urls[mirror]['cloud_mask']
    if mirror=='gcloud':
        bucket_name, *blob_name = url.replace('gs://', '').split('/')
        f = storage.Client().get_bucket(bucket_name).get_blob('/'.join(blob_name))
        gml_content = f.download_as_string()
    else:
        bucket, *key = url.replace('s3://', '').split('/')
        f = boto3.client('s3').get_object(Bucket=bucket, Key='/'.join(key),
                                          RequestPayer='requester')['Body']
        gml_content = f.read()

    clouds = []
    soup = bs4.BeautifulSoup(gml_content, 'xml')
    for polygon in soup.find_all('MaskFeature'):
        if polygon.maskType.text == 'OPAQUE':  # either OPAQUE or CIRRUS
            try:
                coords = list(map(float, polygon.posList.text.split()))
                points = list(zip(coords[::2], coords[1::2]))
                clouds.append(shapely.geometry.Polygon(points))
            except IndexError:
                pass
    aoi_shape = shapely.geometry.shape(aoi)
    try:
        cloudy = shapely.geometry.MultiPolygon(clouds).intersection(aoi_shape)
        return cloudy.area > (p * aoi_shape.area)
    except shapely.geos.TopologicalError:
        return False

def process_clouds(aoi, bands, done_imgs, mirror, parallel_downloads, p=0.5, out_dir=''):
    utils.mkdir_p(os.path.join(out_dir, 'cloudy'))
    cloudy = parallel.run_calls(is_img_cloudy, done_imgs,
                                extra_args=(utils.geojson_lonlat_to_utm(aoi),mirror,p),
                                pool_type='threads',
                                nb_workers=parallel_downloads, verbose=True)
    for img, cloud in zip(done_imgs, cloudy):
        name = img.filename
        if cloud:
            for b in bands:
                f = '{}_band_{}.tif'.format(name, b)
                shutil.move(os.path.join(out_dir, f),
                            os.path.join(out_dir, 'cloudy', f))
    print('{} cloudy images out of {}'.format(sum(cloudy), len(done_imgs)))

def get_time_series(aoi, start_date=None, end_date=None, bands=['B04'],
                    out_dir='', search_api='devseed', mirror='gcloud',
                    product_type=None,
                    parallel_downloads=multiprocessing.cpu_count()):

    check_args(search_api, mirror, product_type)
    imgs = global_search(search_api, aoi, start_date, end_date, product_type)
    download(aoi, bands, imgs, mirror, out_dir, parallel_downloads)
    done_imgs = [img for img in imgs if bands_files_are_valid(img, bands, search_api, out_dir)]
    process_clouds(aoi, bands, done_imgs, mirror, parallel_downloads,out_dir=out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Sentinel-2 images'))
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
    parser.add_argument('-b', '--band', nargs='*', default=['B04'],
                        choices=ALL_BANDS + ['all'], metavar='',
                        help=('space separated list of spectral bands to'
                              ' download. Default is B04 (red). Allowed values'
                              ' are {}'.format(', '.join(ALL_BANDS))))
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('--api', type=str, choices=['devseed', 'planet', 'scihub', 'gcloud'],
                        default='devseed', help='search API')
    parser.add_argument('--mirror', type=str, choices=['aws', 'gcloud'],
                        default='gcloud', help='download mirror')
    parser.add_argument('--product-type', choices=['L1C', 'L2A'], help='type of image')
    parser.add_argument('--parallel-downloads', type=int,
                        default=multiprocessing.cpu_count(),
                        help='max number of parallel crops downloads')
    args = parser.parse_args()

    if 'all' in args.band:
        args.band = ALL_BANDS

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
                    bands=args.band, out_dir=args.outdir, search_api=args.api,
                    mirror=args.mirror,
                    product_type=args.product_type,
                    parallel_downloads=args.parallel_downloads)
