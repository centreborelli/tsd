#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download and crop Planet images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

from __future__ import print_function
import os
import sys
import time
import shutil
import argparse
import multiprocessing
import numpy as np
import utm
import dateutil.parser

import planet

import utils
import parallel
import search_planet

ITEM_TYPES = search_planet.ITEM_TYPES
ASSETS = ['udm',
          'visual',
          'visual_xml',
          'analytic',
          'analytic_xml',
          'analytic_dn',
          'analytic_dn_xml',
          'basic_udm',
          'basic_analytic',
          'basic_analytic_xml',
          'basic_analytic_rpc',
          'basic_analytic_dn',
          'basic_analytic_dn_xml',
          'basic_analytic_dn_rpc']
client = search_planet.client
    

def fname_from_metadata(d):
    """
    Build a string using the image acquisition date and identifier.
    """
    scene_id = d['id']
    date_str = d['properties']['acquired']
    date = dateutil.parser.parse(date_str).date()
    return '{}_scene_{}'.format(date.isoformat(), scene_id)


def metadata_from_metadata_dict(d):
    """
    Return a dict containing some string-formatted metadata.
    """
    imaging_date = dateutil.parser.parse(d['properties']['acquired'])
    sun_zenith = 90 - d['properties']['sun_elevation']  # zenith and elevation are complementary
    sun_azimuth = d['properties']['sun_azimuth']

    out = {
        "IMAGING_DATE": imaging_date.strftime('%Y-%m-%dT%H:%M:%S'),
        "SUN_ZENITH": str(sun_zenith),
        "SUN_AZIMUTH": str(sun_azimuth)
    }
    out.update({str(k): str(d['properties'][k]) for k in d['properties'].keys()})
    return out


def get_download_url(item, asset_type):
    """
    """
    assets = client.get_assets(item).get()

    if asset_type not in assets:
        print("WARNING: no permission to get asset '{}' of {}".format(asset_type,
                                                                     item['_links']['_self']))
        print("\tPermissions for this item are:", item['_permissions'])
        return

    asset = assets[asset_type]
    if asset['status'] == 'inactive':
        activation = client.activate(asset)
        r = activation.response.status_code
        if r != 202:
            print('activation of item {} asset {} returned {}'.format(item['id'],
                                                                      asset_type,
                                                                      r))
        else:
            return get_download_url(item, asset_type)

    elif asset['status'] == 'activating':
        time.sleep(3)
        return get_download_url(item, asset_type)

    elif asset['status'] == 'active':
        return asset['location']


def download_crop(outfile, item, asset, ulx, uly, lrx, lry, utm_zone=None):
    """
    """
    url = get_download_url(item, asset)
    if url is not None:
        if asset.endswith(('_xml', '_rpc')):
            os.system('wget {} -O {}'.format(url, outfile))
        elif asset.startswith('basic'):
            os.system('wget {} -O {}'.format(url, outfile))
        else:
            utils.crop_with_gdal_translate(outfile, url, ulx, uly, lrx, lry,
                                           utm_zone)


def get_time_series(aoi, start_date=None, end_date=None,
                    item_types=['PSScene3Band'], asset_type='analytic',
                    out_dir='',
                    parallel_downloads=multiprocessing.cpu_count()):
    """
    Main function: download and crop of Planet images.
    """
    # list available images
    images = search_planet.search(aoi, start_date, end_date,
                                  item_types=item_types)['features']
    print('Found {} images'.format(len(images)))

    # build filenames
    fnames = [os.path.join(out_dir, '{}.tif'.format(fname_from_metadata(x)))
              for x in images]

    # convert aoi coordinates to utm
    ulx, uly, lrx, lry, utm_zone = utils.utm_bbx(aoi)

    # activate images and download crops
    utils.mkdir_p(out_dir)
    print('Downloading {} crops...'.format(len(images)), end=' ')
    parallel.run_calls('threads', parallel_downloads, 120, True, download_crop,
                       list(zip(fnames, images)), asset_type, ulx, uly, lrx,
                       lry, utm_zone)

    # embed some metadata in the image files
    for f, img in zip(fnames, images):  # embed some metadata as gdal geotiff tags
        if os.path.isfile(f):
            for k, v in metadata_from_metadata_dict(img).items():
                utils.set_geotif_metadata_item(f, k, v)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Planet images'))
    parser.add_argument('--geom', type=utils.valid_geojson,
                        help=('path to geojson file'))
    parser.add_argument('--lat', type=utils.valid_lat,
                        help=('latitude of the center of the rectangle AOI'))
    parser.add_argument('--lon', type=utils.valid_lon,
                        help=('longitude of the center of the rectangle AOI'))
    parser.add_argument('-w', '--width', type=int, default=2000,
                        help='width of the AOI (m), default 2000 m')
    parser.add_argument('-l', '--height', type=int, default=2000,
                        help='height of the AOI (m), default 2000 m')
    parser.add_argument('-s', '--start-date', type=utils.valid_datetime,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_datetime,
                        help='end date, YYYY-MM-DD')
    parser.add_argument('--item-types', nargs='*', choices=ITEM_TYPES,
                        default=['PSScene3Band'], metavar='',
                        help=('space separated list of item types to'
                              ' download. Default is PSScene3Band. Allowed'
                              ' values are {}'.format(', '.join(ITEM_TYPES))))
    parser.add_argument('--asset', default='analytic', metavar='',
                        choices=ASSETS,
                        help=('asset item type to download. Default is analytic.'
                              ' Allowed values are {}'.format(', '.join(ASSETS))))
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-d', '--debug', action='store_true', help=('save '
                                                                    'intermediate '
                                                                    'images'))
    parser.add_argument('--parallel-downloads', type=int, default=10,
                        help='max number of parallel crops downloads')
    args = parser.parse_args()

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
                    item_types=args.item_types, asset_type=args.asset,
                    out_dir=args.outdir, parallel_downloads=args.parallel_downloads)
