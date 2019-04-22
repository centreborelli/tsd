#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic crop and download of Landsat images.

Copyright (C) 2016-18, Carlo de Franchis <carlo.de-franchis@m4x.org>

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
import rasterio
import numpy as np

from tsd import utils
from tsd import parallel
from tsd import l8_metadata_parser

# list of spectral bands
ALL_BANDS = ['B{}'.format(i) for i in range(1,12)] + ['BQA']

def is_image_empty(path, bands):
    band_files = [path.format(band) for band in bands if band!='BQA']
    for band_file in band_files:
        nonzero = rasterio.open(band_file).read().sum()
        # print('There are {} pixels in {}'.format(nonzero, band_file))
        if nonzero==0:
            return True
        else:
            continue
    return False


def check_args(api, mirror):
    if mirror == 'gcloud' and api not in ['gcloud', 'devseed']:
        raise ValueError("ERROR: You must use gcloud or devseed api to use gcloud as mirror")
    if api == 'gcloud':
        try:
            private_key = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        except KeyError:
            raise ValueError('You must have the env variable GOOGLE_APPLICATION_CREDENTIALS linking to the cred json file')


def search(aoi, start_date=None, end_date=None, satellite='L8',
           sensor='OLITIRS', api='devseed'):
    """
    Search Landsat images covering an AOI and timespan using a given API.

    Args:
        aoi (geojson.Polygon): area of interest
        start_date (datetime.datetime): start of the search time range
        end_date (datetime.datetime): end of the search time range
        api (str, optional): either gcloud (default), scihub, planet or devseed
        satellite (str, optional): either L1...L8
        sensor (str, optional): MSS, TM, ETM, OLITIRS
        see https://landsat.usgs.gov/what-are-band-designations-landsat-satellites

    Returns:
        list of image objects
    """
    # list available images
    if api == 'gcloud':
        from tsd import search_gcloud
        images = search_gcloud.search(aoi, start_date, end_date, satellite=satellite, sensor=sensor)
        images = [l8_metadata_parser.LandsatGcloudParser(img) for img in images]
    elif api == 'devseed':
        from tsd import search_devseed
        images = search_devseed.search(aoi, start_date, end_date, satellite='Landsat-8')
        images = [l8_metadata_parser.LandsatDevSeedParser(img) for img in images]

    # sort images by acquisition day, then by mgrs id
    images.sort(key=(lambda k: (k.date.date(), k.row, k.path)))

    print('Found {} images'.format(len(images)))
    return images


def download(imgs, bands, aoi, mirror, out_dir, parallel_downloads):
    """
    Download a timeseries of crops with GDAL VSI feature.

    Args:
        imgs (list): list of images
        bands (list): list of bands
        aoi (geojson.Polygon): area of interest
        mirror (str): either 'aws' or 'gcloud'
        out_dir (str): path where to store the downloaded crops
        parallel_downloads (int): number of parallel downloads
    """
    coords = utils.utm_bbx(aoi)
    crops_args = []
    for img in imgs:
        for b in set(bands + ['BQA']):
            fname = os.path.join(out_dir, '{}_band_{}.tif'.format(img.filename, b))
            crops_args.append((fname, img.urls[mirror][b]))

    out_dir = os.path.abspath(os.path.expanduser(out_dir))
    os.makedirs(out_dir, exist_ok=True)
    print('Downloading {} crops ({} images with {} bands)...'.format(len(crops_args),
                                                                     len(imgs),
                                                                     len(bands) +1),
          end=' ')
    parallel.run_calls(utils.crop_with_gdal_translate, crops_args,
                       extra_args=(*coords,), pool_type='threads',
                       nb_workers=parallel_downloads)


def bands_files_are_valid(img, bands, api, directory):
    """
    Check if all bands images files are valid.
    """
    filenames = ['{}_band_{}.tif'.format(img.filename, b) for b in bands]
    paths = [os.path.join(directory, f) for f in filenames]
    return all(utils.is_valid(p) for p in paths)


def is_image_cloudy(qa_band_file, p=0.5):
    """
    Tell if a Landsat-8 image crop is cloud-covered according to the QA band.

    The crop is considered covered if a fraction larger than p of its pixels are
    labeled as clouds in the QA band.

    Args:
        qa_band_file: path to a Landsat-8 QA band crop
        p: fraction threshold
    """
    with rasterio.open(qa_band_file, 'r') as f:
        x = f.read(1)
    bqa_cloud_yes = [61440, 59424, 57344, 56320, 53248]
    bqa_cloud_maybe = [39936, 36896, 36864]
    mask = np.in1d(x, bqa_cloud_yes + bqa_cloud_maybe).reshape(x.shape)
    return np.count_nonzero(mask) > p * x.size


def read_cloud_masks(imgs, bands, parallel_downloads, p=0.5,
                     out_dir=''):
    """
    Read Landsat-8 cloud masks and intersects them with the input aoi.

    Args:
        imgs (list): list of images
        bands (list): list of bands
        parallel_downloads (int): number of parallel gml files downloads
        p (float): cloud area threshold above which our aoi is said to be
            'cloudy' in the current image
    """
    print('Reading {} cloud masks...'.format(len(imgs)), end=' ')
    names = [img.filename for img in imgs]
    qa_names = [os.path.join(out_dir, '{}_band_BQA.tif'.format(f)) for f in names]
    cloudy = parallel.run_calls(is_image_cloudy, qa_names,
                                pool_type='processes',
                                nb_workers=parallel_downloads, verbose=False)
    print('{} cloudy images out of {}'.format(sum(cloudy), len(imgs)))

    for img, cloud in zip(imgs, cloudy):
        if cloud:
            os.makedirs(os.path.join(out_dir, 'cloudy'), exist_ok=True)
            for b in list(set(bands + ['BQA'])):
                f = '{}_band_{}.tif'.format(img.filename, b)
                shutil.move(os.path.join(out_dir, f),
                            os.path.join(out_dir, 'cloudy', f))


def read_empty_images(imgs, bands, parallel_downloads,
                     out_dir=''):
    """
    Check whether image is empty, since we do not have access to the footprint.

    Args:
        imgs (list): list of images
        bands (list): list of bands
        parallel_downloads (int): number of parallel gml files downloads
    """
    print('Reading {} QA masks...'.format(len(imgs)), end=' ')
    names = [img.filename for img in imgs]
    base_names = [os.path.join(out_dir, '{}_band_{{}}.tif'.format(f)) for f in names]
    cloudy = parallel.run_calls(is_image_empty, base_names,
                                pool_type='processes',
                                extra_args=(bands,),
                                nb_workers=parallel_downloads, verbose=False)
    print('{} empty images out of {}'.format(sum(cloudy), len(imgs)))

    for img, cloud in zip(imgs, cloudy):
        if cloud is True:
            os.makedirs(os.path.join(out_dir, 'empty'), exist_ok=True)
            for b in list(set(bands + ['BQA'])):
                f = '{}_band_{}.tif'.format(img.filename, b)
                shutil.move(os.path.join(out_dir, f),
                            os.path.join(out_dir, 'empty', f))


def get_time_series(aoi, start_date=None, end_date=None, bands=['B8'],
                    satellite='Landsat', sensor=None,
                    out_dir='', api='devseed', mirror='gcloud',
                    cloud_masks=False, check_empty=False,
                    parallel_downloads=multiprocessing.cpu_count()):
    """
    Main function: crop and download a time series of Sentinel-2 images.

    Args:
        aoi (geojson.Polygon): area of interest
        start_date (datetime.datetime, optional): start of the time range
        end_date (datetime.datetime, optional): end of the time range
        bands (list, optional): list of bands
        satellite (str, optional): either L1...L8
        sensor (str, optional): MSS, TM, ETM, OLI
        see https://landsat.usgs.gov/what-are-band-designations-landsat-satellites
        out_dir (str, optional): path where to store the downloaded crops
        api (str, optional): either devseed (default), scihub, planet or gcloud
        mirror (str, optional): either 'aws' or 'gcloud'
        cloud_masks (bool, optional): if True, cloud masks are downloaded and
            cloudy images are discarded
        check_empty (bool, optional): if True, QA masks are downloaded and
            empty images are discarded
        parallel_downloads (int): number of parallel gml files downloads
    """
    # check access to the selected search api and download mirror
    check_args(api, mirror)

    # default date range
    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(91)  # 3 months

    # list available images
    images = search(aoi, start_date, end_date, satellite, sensor, api=api)

    # download crops
    download(images, bands, aoi, mirror, out_dir, parallel_downloads)

    # discard images that failed to download
    images = [i for i in images if bands_files_are_valid(i, bands, api, out_dir)]

    # embed all metadata as GeoTIFF tags in the image files
    for img in images:
        metadata = vars(img)
        metadata['downloaded_by'] = 'TSD on {}'.format(datetime.datetime.now().isoformat())
        for b in bands:
            filepath = os.path.join(out_dir, '{}_band_{}.tif'.format(img.filename, b))
            utils.set_geotif_metadata_items(filepath, metadata)

    if cloud_masks:  # discard images that are totally covered by clouds
        read_cloud_masks(images, bands, parallel_downloads,
                         out_dir=out_dir)

    if check_empty:  # discard images that are totally empty
        read_empty_images(images, bands, parallel_downloads,
                         out_dir=out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Landsat images'))
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
    parser.add_argument('-b', '--band', nargs='*', default=['B8'],
                        choices=ALL_BANDS + ['all'], metavar='',
                        help=('space separated list of spectral bands to'
                              ' download. Default is 8 (panchro). Allowed values'
                              ' are {}'.format(', '.join(ALL_BANDS))))
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('--api', type=str, choices=['devseed', 'planet', 'scihub', 'gcloud'],
                        default='devseed', help='search API')
    parser.add_argument('--mirror', type=str, choices=['aws', 'gcloud'],
                        default='gcloud', help='download mirror')
    parser.add_argument('--satellite', type=str, choices=['Landsat', 'Landsat-4', 'Landsat-5', 'Landsat-6', 'Landsat-7', 'Landsat-8'],
                        default='Landsat', help='satellite')
    parser.add_argument('--sensor', type=str, choices=['MSS', 'TM', 'ETM', 'OLITIRS'],
                        default=None, help='sensor')
    parser.add_argument('--parallel-downloads', type=int,
                        default=multiprocessing.cpu_count(),
                        help='max number of parallel crops downloads')
    parser.add_argument('--cloud-masks',  action='store_true',
                        help=('download cloud masks crops from provided GML files'))
    parser.add_argument('--check-empty',  action='store_true',
                        help=('download QA masks to remove empty images'))
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
                    bands=args.band,
                    satellite=args.satellite, sensor=args.sensor,
                    out_dir=args.outdir, api=args.api,
                    mirror=args.mirror,
                    cloud_masks=args.cloud_masks,
                    check_empty=args.check_empty,
                    parallel_downloads=args.parallel_downloads)
