#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download, crop, registration, filtering, and equalization of Landsat
images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

from __future__ import print_function
import os
import sys
import shutil
import argparse
import datetime
import numpy as np
import utm
import dateutil.parser
import tifffile

import utils
import parallel
import search_devseed
import search_planet
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from stable.scripts.midway import midway_on_files
from stable.scripts import registration


aws_url = 'http://landsat-pds.s3.amazonaws.com'  # https://landsatonaws.com/


def aws_url_from_metadata_dict(d, api='devseed'):
    """
    Build the AWS url of a Landsat image from it's metadata.
    """
    if api == 'devseed':
        path = d['path']
        row = d['row']
        scene_id = d['sceneID']
    elif api == 'planet':
        path = d['properties']['wrs_path']
        row = d['properties']['wrs_row']
        scene_id = d['id']
    return '{0}/L8/{1:03d}/{2:03d}/{3}/{3}'.format(aws_url, path, row, scene_id)


def filename_from_metadata_dict(d, api='devseed'):
    """
    Build a string using the image acquisition date and identifier.
    """
    if api == 'devseed':
        scene_id = d['sceneID']
        date_str = d['date']
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
        date = dateutil.parser.parse(d['date']).date()
        time = datetime.time(*map(int, d['sceneStartTime'].split(':')[2:4]))
        imaging_date = datetime.datetime.combine(date, time)
        sun_zenith = 90 - d['sunElevation']  # zenith and elevation are complementary
        sun_azimuth = d['sunAzimuth']
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
    x = tifffile.imread(qa_band_file)
    bqa_cloud_yes = [61440, 59424, 57344, 56320, 53248]
    bqa_cloud_maybe = [39936, 36896, 36864]
    mask = np.in1d(x, bqa_cloud_yes + bqa_cloud_maybe).reshape(x.shape)
    return np.count_nonzero(mask) > p * x.size


def get_time_series(lat, lon, w, h, start_date=None, end_date=None, bands=[8],
                    out_dir='', search_api='devseed', parallel_downloads=10,
                    register=False, equalize=False, debug=False):
    """
    Main function: download, crop and register a time series of Landsat-8 images.
    """
    # list available images
    if search_api == 'devseed':
        images = search_devseed.search(lat, lon, w, h, start_date,
                                       end_date)['results']
    elif search_api == 'planet':
        images = search_planet.search(lat, lon, w, h, start_date, end_date,
                                      item_types=['Landsat8L1G'])['features']
    print('Found {} images'.format(len(images)))

    # build urls and filenames
    urls = []
    fnames = []
    for img in images:
        url = aws_url_from_metadata_dict(img, search_api)
        name = filename_from_metadata_dict(img, search_api)
        for b in set(bands + ['QA']):  # the QA band is needed for cloud detection
            urls.append('/vsicurl/{}_B{}.TIF'.format(url, b))
            fnames.append(os.path.join(out_dir, '{}_band_{}.tif'.format(name, b)))

    # compute coordinates of the crop
    cx, cy = utm.from_latlon(lat, lon)[:2]

    if register:  # take 100 meters margin in case of forthcoming shift
        w += 100
        h += 100

    ulx = cx - w / 2
    lrx = cx + w / 2
    uly = cy + h / 2  # in UTM the y coordinate increases from south to north
    lry = cy - h / 2

    # download crops
    utils.mkdir_p(out_dir)
    print('Downloading {} crops ({} images with {} bands)...'.format(len(urls),
                                                                     len(images),
                                                                     len(bands) + 1))
    parallel.run_calls(utils.download_crop_with_gdal_vsicurl, zip(fnames, urls),
                       parallel_downloads, ulx, uly, lrx, lry)

    # discard images that are totally covered by clouds
    cloudy = []
    for img in images:
        name = filename_from_metadata_dict(img, search_api)
        if is_image_cloudy(os.path.join(out_dir, '{}_band_QA.tif'.format(name))):
            cloudy.append(img)
            utils.mkdir_p(os.path.join(out_dir, 'cloudy'))
            for b in bands + ['QA']:
                f = '{}_band_{}.tif'.format(name, b)
                shutil.move(os.path.join(out_dir, f),
                            os.path.join(out_dir, 'cloudy', f))
    print('{} cloudy images out of {}'.format(len(cloudy), len(images)))
    for x in cloudy:
        images.remove(x)

    # group band crops per image
    crops = []  # list of lists: [[crop1_b1, crop1_b2 ...], [crop2_b1 ...] ...]
    for img in images:
        name = filename_from_metadata_dict(img, search_api)
        crops.append([os.path.join(out_dir, '{}_band_{}.tif'.format(name, b))
                      for b in bands])

    # embed some metadata in the remaining image files
    for bands_fnames in crops:
        for f in bands_fnames:  # embed some metadata as gdal geotiff tags
            for k, v in metadata_from_metadata_dict(img, search_api).items():
                utils.set_geotif_metadata_item(f, k, v)

    # register the images through time
    if register:
        if debug:  # keep a copy of the cropped images before registration
            bak = os.path.join(out_dir, 'no_registration')
            utils.mkdir_p(bak)
            for bands_fnames in crops:
                for f in bands_fnames:  # crop to remove the margin
                    o = os.path.join(bak, os.path.basename(f))
                    utils.crop_georeferenced_image(o, f, lon, lat, w-100, h-100)

        print('Registering...')
        registration.main(crops, crops, all_pairwise=True)

        for bands_fnames in crops:
            for f in bands_fnames:  # crop to remove the margin
                utils.crop_georeferenced_image(f, f, lon, lat, w-100, h-100)

    # equalize histograms through time, band per band
    if equalize:
        if debug:  # keep a copy of the images before equalization
            bak = os.path.join(out_dir, 'no_midway')
            utils.mkdir_p(bak)
            for bands_fnames in crops:
                for f in bands_fnames:
                    shutil.copy(f, bak)

        print('Equalizing...')
        for i in xrange(len(bands)):
            midway_on_files([crop[i] for crop in crops if len(crop) > i], out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Landsat images'))
    parser.add_argument('--lat', type=utils.valid_lat, required=True,
                        help='latitude')
    parser.add_argument('--lon', type=utils.valid_lon, required=True,
                        help='longitude')
    parser.add_argument('-w', '--width', type=int, help='width of the crop, in meters',
                        default=5000)
    parser.add_argument('-l', '--height', type=int, help='height of the crop, in meters',
                        default=5000)
    parser.add_argument('-s', '--start-date', type=utils.valid_date,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_date,
                        help='end date, YYYY-MM-DD')
    parser.add_argument('-b', '--band', nargs='*', default=[8],
                        help=('list of spectral bands, default band 8 (panchro)'))
    parser.add_argument('-r', '--register', action='store_true',
                        help='register images through time')
    parser.add_argument('-m', '--midway', action='store_true',
                        help='equalize colors with midway')
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-d', '--debug', action='store_true', help=('save '
                                                                    'intermediate '
                                                                    'images'))
    parser.add_argument('--api', type=str, default='devseed',
                        help='search API')
    parser.add_argument('--parallel-downloads', type=int, default=10,
                        help='max number of parallel crops downloads')
    args = parser.parse_args()

    get_time_series(args.lat, args.lon, args.width, args.height,
                    start_date=args.start_date, end_date=args.end_date,
                    bands=args.band, register=args.register,
                    equalize=args.midway, out_dir=args.outdir,
                    debug=args.debug, search_api=args.api,
                    parallel_downloads=args.parallel_downloads)
