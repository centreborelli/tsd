#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic crop and download of Landsat timeseries.

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

from __future__ import print_function
import os
import sys
import shutil
import argparse
import datetime
import numpy as np
import utm
import boto3
import botocore
import dateutil.parser
import requests
import rasterio

import search_devseed
import utils
import parallel


# https://landsatonaws.com/
AWS_HTTP_URL = 'http://landsat-pds.s3.amazonaws.com'
AWS_S3_URL = 's3://landsat-pds'

# list of spectral bands
ALL_BANDS = ['1', '2', '3', '4', '5', '6', '7', '8', '9',
             '10', '11', 'QA']

def we_can_access_aws_through_s3():
    """
    Test if we can access AWS through s3.
    """
    if 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
        try:
            boto3.session.Session().client('s3').list_objects_v2(Bucket=AWS_S3_URL[5:])
            return True
        except botocore.exceptions.ClientError:
            pass
    return False


WE_CAN_ACCESS_AWS_THROUGH_S3 = we_can_access_aws_through_s3()


def google_url_from_metadata_dict_backend(d, api='devseed'):
    """
    Build the Google url of a Landsat image from it's metadata.
    """
    if api == 'devseed':
        return d['download_links']['google'][0].rsplit('_', 1)[0]


def google_url_from_metadata_dict(d, api='devseed', band=None):
    """
    Build the Google url of a Landsat image from it's metadata.
    """
    baseurl = google_url_from_metadata_dict_backend(d, api)
    if band and band in ALL_BANDS:
        return '{}_B{}.TIF'.format(baseurl, band)
    else:
        return baseurl


def aws_paths_from_metadata_dict(d, api='devseed'):
    """
    Build the AWS paths of the 12 bands of a Landsat-8 image from its metadata.
    """
    if api == 'devseed':
        # remove the http://landsat-pds.s3.amazonaws.com/ prefix from the urls
        return ['/'.join(d['assets']['B{}'.format(b)]['href'].split('/')[3:]) for b in ALL_BANDS]
        # Is this issue still a problem?
        # https://github.com/sat-utils/landsat8-metadata/issues/6
    elif api == 'planet':  # currently broken
        col = d['properties']['wrs_path']
        row = d['properties']['wrs_row']
        scene_id = d['id']
        return 'L8/{0:03d}/{1:03d}/{2}/{2}'.format(col, row, scene_id)


def aws_urls_from_metadata_dict(d, api='devseed'):
    """
    Build the AWS s3 or http urls of the 12 bands of a Landsat-8 image.
    """
    if WE_CAN_ACCESS_AWS_THROUGH_S3:
        aws_url = AWS_S3_URL
    else:
        aws_url = AWS_HTTP_URL
    return ['{}/{}'.format(aws_url, p) for p in aws_paths_from_metadata_dict(d, api)]


def filename_from_metadata_dict(d, api='devseed'):
    """
    Build a string using the image acquisition date and identifier.
    """
    if api == 'devseed':
        scene_id = d['properties']['id']
        date_str = d['properties']['datetime']
    elif api == 'planet':
        scene_id = d['id']
        date_str = d['properties']['acquired']
    date = dateutil.parser.parse(date_str).date()
    return '{}_scene_{}'.format(date.isoformat(), scene_id)


def metadata_from_metadata_dict(d, api='devseed'):
    """
    Return a dict containing some string-formatted metadata.
    """
    if api == 'devseed':
        imaging_date = dateutil.parser.parse(d['properties']['datetime'])
        sun_zenith = 90 - d['properties']['eo:sun_elevation']  # zenith and elevation are complementary
        sun_azimuth = d['properties']['eo:sun_azimuth']
    elif api == 'planet':
        imaging_date = dateutil.parser.parse(d['properties']['acquired'])
        sun_zenith = 90 - d['properties']['sun_elevation']  # zenith and elevation are complementary
        sun_azimuth = d['properties']['sun_azimuth']

    return {
        "IMAGING_DATE": imaging_date.strftime('%Y-%m-%dT%H:%M:%S'),
        "SUN_ZENITH": str(sun_zenith),
        "SUN_AZIMUTH": str(sun_azimuth)
    }


def is_image_cloudy(qa_band_file, p=.5):
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


def bands_files_are_valid(img, bands, search_api, directory):
    """
    Check if all bands images files are valid.
    """
    name = filename_from_metadata_dict(img, search_api)
    filenames = ['{}_band_{}.tif'.format(name, b) for b in bands]
    paths = [os.path.join(directory, f) for f in filenames]
    return all(utils.is_valid(p) for p in paths)


def get_time_series(aoi, start_date=None, end_date=None, bands=[8],
                    out_dir='', search_api='devseed', parallel_downloads=100,
                    debug=False):
    """
    Main function: crop and download a time series of Landsat-8 images.
    """
    utils.print_elapsed_time.t0 = datetime.datetime.now()

    # list available images
    seen = set()
    if search_api == 'devseed':
        images = search_devseed.search(aoi, start_date, end_date,
                                       'Landsat-8')['features']
        images.sort(key=lambda k: (k['properties']['datetime'],
                                   k['properties']['landsat:row'],
                                   k['properties']['landsat:path']))

    elif search_api == 'planet':
        import search_planet
        images = search_planet.search(aoi, start_date, end_date,
                                      item_types=['Landsat8L1G'])

        # sort images by acquisition date, then by acquisiton row and path
        images.sort(key=lambda k: (k['properties']['acquired'],
                                   k['properties']['wrs_row'],
                                   k['properties']['wrs_path']))

        # remove duplicates (same acquisition day)
        images = [x for x in images if not (x['properties']['acquired'] in seen
                                            or  # seen.add() returns None
                                            seen.add(x['properties']['acquired']))]
    print('Found {} images'.format(len(images)))
    utils.print_elapsed_time()

    # build urls
    urls = parallel.run_calls(aws_urls_from_metadata_dict, list(images),
                              extra_args=(search_api,), pool_type='threads',
                              nb_workers=parallel_downloads, verbose=False)

    # build gdal urls and filenames
    download_urls = []
    fnames = []
    for img, bands_urls in zip(images, urls):
        name = filename_from_metadata_dict(img, search_api)
        for b in set(bands + ['QA']):  # the QA band is needed for cloud detection
            download_urls += [s for s in bands_urls if s.endswith('B{}.TIF'.format(b))]
            fnames.append(os.path.join(out_dir, '{}_band_{}.tif'.format(name, b)))

    # convert aoi coordinates to utm
    ulx, uly, lrx, lry, utm_zone, lat_band = utils.utm_bbx(aoi)

    # download crops
    utils.mkdir_p(out_dir)
    print('Downloading {} crops ({} images with {} bands)...'.format(len(download_urls),
                                                                     len(images),
                                                                     len(bands) + 1),
         end=' ')
    parallel.run_calls(utils.crop_with_gdal_translate, list(zip(fnames, download_urls)),
                       extra_args=(ulx, uly, lrx, lry, utm_zone, lat_band),
                       pool_type='threads', nb_workers=parallel_downloads)
    utils.print_elapsed_time()

    # discard images that failed to download
    images = [x for x in images if bands_files_are_valid(x, list(set(bands + ['QA'])),
                                                         search_api, out_dir)]
    # discard images that are totally covered by clouds
    utils.mkdir_p(os.path.join(out_dir, 'cloudy'))
    names = [filename_from_metadata_dict(img, search_api) for img in images]
    qa_names = [os.path.join(out_dir, '{}_band_QA.tif'.format(f)) for f in names]
    cloudy = parallel.run_calls(is_image_cloudy, qa_names,
                                pool_type='processes',
                                nb_workers=parallel_downloads, verbose=False)
    for name, cloud in zip(names, cloudy):
        if cloud:
            for b in list(set(bands + ['QA'])):
                f = '{}_band_{}.tif'.format(name, b)
                shutil.move(os.path.join(out_dir, f),
                            os.path.join(out_dir, 'cloudy', f))
    print('{} cloudy images out of {}'.format(sum(cloudy), len(images)))
    images = [i for i, c in zip(images, cloudy) if not c]
    utils.print_elapsed_time()

    # group band crops per image
    crops = []  # list of lists: [[crop1_b1, crop1_b2 ...], [crop2_b1 ...] ...]
    for img in images:
        name = filename_from_metadata_dict(img, search_api)
        crops.append([os.path.join(out_dir, '{}_band_{}.tif'.format(name, b))
                      for b in bands])

    # embed some metadata in the remaining image files
    for bands_fnames in crops:
        for f in bands_fnames:  # embed some metadata as gdal geotiff tags
            utils.set_geotif_metadata_items(f, metadata_from_metadata_dict(img, search_api))


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
    parser.add_argument('-b', '--band', nargs='*', default=['8'],
                        choices=ALL_BANDS + ['all'], metavar='',
                        help=('space separated list of spectral bands to'
                              ' download. Default is 8 (panchro). Allowed values'
                              ' are {}'.format(', '.join(ALL_BANDS))))
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-d', '--debug', action='store_true', help=('save '
                                                                    'intermediate '
                                                                    'images'))
    parser.add_argument('--api', type=str, choices=['devseed', 'planet', 'scihub'],
                        default='devseed', help='search API')
    parser.add_argument('--parallel-downloads', type=int, default=100,
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
                    bands=args.band, out_dir=args.outdir, debug=args.debug,
                    search_api=args.api,
                    parallel_downloads=args.parallel_downloads)
