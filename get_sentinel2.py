#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download, crop, registration, filtering, and equalization of
Sentinel-2 images.

Copyright (C) 2016, Carlo de Franchis <carlo.de-franchis@m4x.org>
Copyright (C) 2016, Axel Davy <axel.davy@ens.fr>

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
import fnmatch
import re
import bs4
import csv
import shutil
import operator
import argparse
import datetime
import subprocess
import dateutil.parser

import utm
import mgrs
import requests
import tifffile
import matplotlib.path
import matplotlib.transforms
import numpy as np
import scipy.ndimage
import weightedstats

import search_sentinel2
import download_sentinel2
import registration
import utils

from sortedcontainers import SortedSet

# http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com

cache_dir = os.path.abspath('.s2-cache')
all_bands = ['01', '02', '03', '04', '05', '06', '07', '08', '8A', '09', '10',
             '11', '12']


def get_time_series(lat, lon, bands, w, h, register=False, out_dir='',
                    start_date=None, end_date=None, sen2cor=False,
                    api='kayrros'):
    """
    Main function: download, crop and register a Sentinel-2 image time series.
    """
    # list available images that are not empty or masked by clouds
    images = search_sentinel2.list_usable_images(lat, lon, start_date, end_date)

    # take 100 meters margin in case of forthcoming shift
    if register:
        w += 100
        h += 100

    # download images
    crops = []
    for img in images:
        if api == 'kayrros':
            l = download_sentinel2.get_crops_from_kayrros_api(img, bands, lon,
                                                              lat, w, h, out_dir)
        else:
            l = download_sentinel2.get_crops_from_aws(img, bands, lon, lat, w,
                                                      h, out_dir)
        if l:
            crops.append(l)

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

#    # produce color images
#    for img in images:
#        if '02' in bands and '03' in bands and '04' in bands:  # B, G, R
#            rgb = '{}_rgb.tif'.format(img['name'])
#            utils.merge_bands([img['02'], img['03'], img['04']],
#                              os.path.join(out_dir, rgb))
#        if '03' in bands and '04' in bands and '08' in bands:  # G, R, I
#            irg = '{}_irg.tif'.format(img['name'])
#            utils.merge_bands([img['03'], img['04'], img['08']],
#                              os.path.join(out_dir, irg))


def get_available_dates_for_coords(lats, lons, union_intersect=False, start_date=None, end_date=None):
    """
    Returns dates of available images at given coordinates.

    Args:
        lats: 1D table of latitudes
        lons: 1D table of longitudes
        union_intersect: There may be different dates available depending
        on the coordinates. Pick the union of these dates (union_intersect = False)
        or the intersection (union_intersect = True)
        start_date/end_date: date boundaries

    Returns:
        list of datetimes
    """
    if start_date is None:
        start_date = datetime.datetime(2000, 01, 01)
    if end_date is None:
        end_date = datetime.datetime.now()

    # list of available images in the requested date range
    res = SortedSet()
    init = False
    for (lat, lon) in zip(lats, lons):
        current = SortedSet()
        for x in search_for_sentinel_images_on_aws(lat, lon):
            date = datetime.datetime(*map(int, x.split('/')[7:10]))
            if start_date < date < end_date:
                current.add(date)
        if not(init):
            res = current
            init = True
        elif union_intersect:
            res.intersection_update(current)
        else:
            res.update(current)
    return [d for d in res]


def get_images_for_date(lat, lon, date, bands, crop_size):
    """
    Assumes is called from get_images_for_dates who has already
    done some caching.

    If there was no image for the selected date,
    returns None. TODO If the image was cloudy, returns False.
    Else returns a 3D array: crop_size x crop_size x num_bands
    """

    res = np.zeros((crop_size, crop_size, len(bands)))
    for (b,i) in zip(bands, range(len(bands))):
        cdir = 'cache/{}_{}/'.format(lat,lon)
        imgname_pattern = 'tile_*_acquired_{}_band_{}.tif'.format(date.date(), b) #ignore mgrs_id
        files = os.listdir(cdir)
        files = fnmatch.filter(files, imgname_pattern)
        if len(files) > 1:
            print ('Warning: several matching files')
            print (files)
        if (len(files) == 0): # no file. cloudy/missing date
            return None
        f = cdir + files[0]
        img = tifffile.imread(f)
        assert(not(f is None))
        if b in ['01', '09', '10', '05', '06', '07', '8A', '11', '12']:
            img = scipy.ndimage.zoom(img, (crop_size, crop_size), order=0)
        else:
            if img.shape != (crop_size, crop_size):
                print (img.shape)
                assert (False)
        res[:,:,i] = img[:,:]

    return res


def get_images_for_dates(lat, lon, dates, bands, crop_size=246):
    """
    Returns the selected bands at selected dates.

    Args:
        lat: latitude
        lon: longitude
        dates: list of datetimes
        bands: The selected bands
        crop_size: width of the square crop

    Returns: List of 3d arrays. Each list element corresponds
    to a date. If there was no image for the selected date, the
    item is None. TODO If the image was cloudy, the item is False.
    Else contains a 3D array: crop_size x crop_size x num_bands
    """
    cdir = 'cache/{}_{}/'.format(lat,lon)
    utils.mkdir_p('cache')
    utils.mkdir_p(cdir)
    if len(os.listdir(cdir)) == 0:
        get_time_series(lat, lon, all_bands, crop_size, crop_size,
                        register=True,
                        out_dir='cache/{}_{}/'.format(lat,lon))
    res = [get_images_for_date(lat, lon, date, bands, crop_size) for date in dates]
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Sentinel-2 images'))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--csv', type=str, help=('path to a csv file containing '
                                                'a list of api, lon, lat'))
    group.add_argument('--latlon', nargs=2, type=float, help=('latitude and '
                                                              'longitude of the '
                                                              'interest point'))
    parser.add_argument('-s', '--start-date', type=utils.valid_date,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_date,
                        help='end date, YYYY-MM-DD')
    parser.add_argument("-b", "--band", nargs='*',
                        help=("list of spectral bands, default all 13 bands"))
    parser.add_argument("-r", "--register", action="store_true",
                        help="register images through time")
    parser.add_argument('-w', '--wsize', type=int, help='size of the crop, in meters',
                        default=5000)
    parser.add_argument('-d', '--dir', type=str, help=('path to save the '
                                                       'images'), default='')
    parser.add_argument("--use-sen2cor", action="store_true",
                        help="apply Sen2Cor Scene Classification")
    parser.add_argument('--api', type=str, default='kayrros',
                        help='API used: kayrros or cmla')
    parser.add_argument('--cache', type=str, help=('cache directory'))
    args = parser.parse_args()

    # cache directory
    if args.cache is not None:
        cache_dir = args.cache

    # list of bands as strings
    if args.band is None:
        bands = all_bands
    else:
        bands = [str(b).zfill(2).upper() for b in args.band]

    if args.latlon:
        get_time_series(args.latlon[0], args.latlon[1], bands, args.wsize,
                        args.wsize, args.register, out_dir=args.dir,
                        start_date=args.start_date, end_date=args.end_date,
                        sen2cor=args.use_sen2cor, api=args.api)
    else:
        # open CSV file and process the entries one at a time
        with open(args.csv) as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))  # autodetect sep
            csvfile.seek(0)
            header = csv.Sniffer().has_header(csvfile.read(1024))  # autodetect header
            csvfile.seek(0)
            reader = csv.reader(csvfile, dialect)
            if header:
                reader.next()  # skip header row
            for row in reader:
                api = row[0]
                lon = float(row[1])
                lat = float(row[2])
                start_date = dateutil.parser.parse(row[3]) if len(row) > 3 else args.start_date
                end_date = dateutil.parser.parse(row[4]) if len(row) > 4 else args.end_date
                print("processing API {}...".format(api), end=' ')
                get_time_series(lat, lon, bands, args.wsize, args.wsize,
                                args.register,
                                out_dir=os.path.join(args.dir, api),
                                start_date=start_date, end_date=end_date,
                                sen2cor=args.use_sen2cor,
                                api=args.api)


#        # compute scene classification
#        if sen2cor:
#            crop = os.path.join(out_dir, '{}_scene_classification.tif'.format(name))
#            if not os.path.isfile(crop) or not utils.is_valid(crop):
#                scl = os.path.join(cache_dir, '{}_scene_classification.tif'.format(name))
#                if not os.path.isfile(scl) or not utils.is_valid(scl):
#                    download_image_bands_aws(url, all_bands, cache_dir)
#                    scene_classification(scl, url)
#                utils.crop_georeferenced_image(crop, img, lon, lat, w, h)
#            img_info['scl'] = crop
