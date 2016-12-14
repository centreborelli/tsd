#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download, crop, registration, filtering, and equalization of Landsat
images.

Copyright (C) 2016, Carlo de Franchis <carlo.de-franchis@m4x.org>

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of the University of California, Berkeley nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from __future__ import print_function
import os
import shutil
import argparse
import tifffile

import search_landsat
import download_landsat
import registration
import utils


all_bands = [str(i) for i in xrange(1, 12)]


def get_time_series(lat, lon, bands, w, h, register=False, out_dir='',
                    start_date=None, end_date=None):
    """
    Main function: download, crop and register a Sentinel-2 image time series.
    """
    # list available images that are not empty or masked by clouds
    images = search_landsat.list_images(lat, lon, start_date, end_date)

    # take 100 meters margin in case of forthcoming shift
    if register:
        w += 100
        h += 100

    # download images
    crops = []
    for img in images:
        d = os.path.join(out_dir, img['date'].isoformat())
        l = download_landsat.get_crops_from_kayrros_api(img, bands, lon, lat,
                                                          w, h, d)
        if l:
            if all(tifffile.imread(x).any() for x in l):  # test for empty images
                crops.append(l)
            else:
                shutil.rmtree(os.path.dirname(l[0]))

    # register the images through time
    if register:
        tmp = [['{}_registered{}'.format(*os.path.splitext(b)) for b in c] for c
               in crops]
        registration.main(crops, tmp, all_pairwise=True)

        # crop to remove the margin
        for crop in crops:
            for band in crop:
                band_reg = '{}_registered{}'.format(*os.path.splitext(band))
                if os.path.isfile(band_reg):
                    utils.crop_georeferenced_image(band, band_reg, lon, lat, w-100, h-100)
                    os.remove(band_reg)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Landsat images'))
    parser.add_argument('--lat', type=float, required=True, help='latitude')
    parser.add_argument('--lon', type=float, required=True, help='longitude')
    parser.add_argument('-s', '--start-date', type=utils.valid_date,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_date,
                        help='end date, YYYY-MM-DD')
    parser.add_argument("-b", "--band", nargs='*',
                        help=("list of spectral bands, default all 11 bands"),
                        default=all_bands)
    parser.add_argument("-r", "--register", action="store_true",
                        help="register images through time")
    parser.add_argument('-w', '--wsize', type=int, help='size of the crop, in meters',
                        default=5000)
    parser.add_argument('-d', '--dir', type=str, help=('path to save the '
                                                       'images'), default='')
    args = parser.parse_args()

    get_time_series(args.lat, args.lon, args.band, args.wsize, args.wsize,
                    args.register, out_dir=args.dir, start_date=args.start_date,
                    end_date=args.end_date)
