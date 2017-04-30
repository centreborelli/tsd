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
import shutil
import argparse
import tifffile

import search_devseed
import download_landsat
import utils

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from stable.scripts.midway import midway_on_files
from stable.scripts import registration


def get_time_series(lat, lon, bands, w, h, register=False, equalize=False,
                    out_dir='', start_date=None, end_date=None, debug=False,
                    mirror='aws'):
    """
    Main function: download, crop and register a time series of Landsat-8 images.
    """
    # list available images
    images = search_devseed.search(lat, lon, w, h, start_date,
                                   end_date)['results']

    if register:  # take 100 meters margin in case of forthcoming shift
        w += 100
        h += 100

    # download images
    crops = []
    for img in images:
        l = download_landsat.get_crops(img, bands, lon, lat, w, h, out_dir,
                                       mirror)
        if l:
            crops.append(l)

    # register the images through time
    if register:
        if debug:  # keep a copy of the cropped images before registration
            bak = os.path.join(out_dir, 'no_registration')
            utils.mkdir_p(bak)
            for crop in crops:  # crop to remove the margin
                for b in crop:
                    o = os.path.join(bak, os.path.basename(b))
                    utils.crop_georeferenced_image(o, b, lon, lat, w-100, h-100)

        registration.main(crops, crops, all_pairwise=True)

        for crop in crops:  # crop to remove the margin
            for b in crop:
                utils.crop_georeferenced_image(b, b, lon, lat, w-100, h-100)

    # equalize histograms through time, band per band
    if equalize:
        if debug:  # keep a copy of the images before equalization
            utils.mkdir_p(os.path.join(out_dir, 'no_midway'))
            for crop in crops:
                for b in crop:
                    shutil.copy(b, os.path.join(out_dir, 'no_midway'))

        for i in xrange(len(bands)):
            midway_on_files([crop[i] for crop in crops if len(crop) > i], out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Landsat images'))
    parser.add_argument('--lat', type=utils.valid_lat, required=True,
                        help='latitude')
    parser.add_argument('--lon', type=utils.valid_lon, required=True,
                        help='longitude')
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
    parser.add_argument('-w', '--width', type=int, help='width of the crop, in meters',
                        default=5000)
    parser.add_argument('-l', '--height', type=int, help='height of the crop, in meters',
                        default=5000)
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-d', '--debug', action='store_true', help=('save '
                                                                    'intermediate '
                                                                    'images'))
    parser.add_argument('--mirror', type=str, default='aws',
                        help='mirror from where to download: aws or kayrros')
    args = parser.parse_args()

    get_time_series(args.lat, args.lon, args.band, args.width, args.height,
                    args.register, args.midway, out_dir=args.outdir,
                    start_date=args.start_date, end_date=args.end_date,
                    debug=args.debug, mirror=args.mirror)
