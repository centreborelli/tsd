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
import utm
import dateutil.parser

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


def get_time_series(lat, lon, w, h, start_date=None, end_date=None, bands=[8],
                    register=False, equalize=False, out_dir='', debug=False,
                    search_api='devseed', download_mirror='aws'):
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

    # build urls and filenames
    urls = []
    fnames = []
    for img in images:
        url = aws_url_from_metadata_dict(img, search_api)
        name = filename_from_metadata_dict(img, search_api)
        for b in bands:
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
    print('Downloading {} image crops...'.format(len(urls)))
    parallel.run_calls(utils.download_crop_with_gdal_vsicurl, zip(fnames,
                                                                  urls), 10,
                       ulx, uly, lrx, lry)

    # embed some metadata in the image files
    crops = []  # list of lists: [[crop1_b1, crop1_b2 ...], [crop2_b1 ...] ...]
    for img in images:
        name = filename_from_metadata_dict(img, search_api)
        bands_fnames = [os.path.join(out_dir, '{}_band_{}.tif'.format(name, b)) for b in bands]
        for f in bands_fnames:  # embed some metadata as gdal geotiff tags
            for k, v in metadata_from_metadata_dict(img, search_api).items():
                utils.set_geotif_metadata_item(f, k, v)
        crops.append(bands_fnames)

    # register the images through time
    if register:
        if debug:  # keep a copy of the cropped images before registration
            bak = os.path.join(out_dir, 'no_registration')
            utils.mkdir_p(bak)
            for f in fnames:  # crop to remove the margin
                o = os.path.join(bak, os.path.basename(f))
                utils.crop_georeferenced_image(o, f, lon, lat, w-100, h-100)

        registration.main(crops, crops, all_pairwise=True)

        for f in fnames:  # crop to remove the margin
            utils.crop_georeferenced_image(f, f, lon, lat, w-100, h-100)

    # equalize histograms through time, band per band
    if equalize:
        if debug:  # keep a copy of the images before equalization
            bak = os.path.join(out_dir, 'no_midway')
            utils.mkdir_p(bak)
            for f in fnames:
                shutil.copy(f, bak)

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
    parser.add_argument("-b", "--band", nargs='*', default=[8],
                        help=("list of spectral bands, default band 8 (panchro)"))
    parser.add_argument("-r", "--register", action="store_true",
                        help="register images through time")
    parser.add_argument('-m', '--midway', action='store_true',
                        help='equalize colors with midway')
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-d', '--debug', action='store_true', help=('save '
                                                                    'intermediate '
                                                                    'images'))
    parser.add_argument('--mirror', type=str, default='aws',
                        help='mirror from where to download')
    parser.add_argument('--api', type=str, default='devseed',
                        help='search API')
    args = parser.parse_args()

    get_time_series(args.lat, args.lon, args.width, args.height,
                    start_date=args.start_date, end_date=args.end_date,
                    bands=args.band, register=args.register,
                    equalize=args.midway, out_dir=args.outdir,
                    debug=args.debug, search_api=args.api,
                    download_mirror=args.mirror)
