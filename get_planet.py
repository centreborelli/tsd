#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download and crop of Planet images.

Copyright (C) 2016-18, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

from __future__ import print_function
import os
import time
import json
import argparse
import multiprocessing

import area
import requests
import numpy as np
import dateutil.parser

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
          'analytic_sr',
          'basic_udm',
          'basic_analytic',
          'basic_analytic_xml',
          'basic_analytic_rpc',
          'basic_analytic_dn',
          'basic_analytic_dn_xml',
          'basic_analytic_dn_rpc']
client = search_planet.client

cas_url = 'https://api.planet.com/compute/ops/clips/v1'  # clip and ship
quota_url = 'https://api.planet.com/auth/v1/experimental/public/my/subscriptions'


def get_quota():
    """
    Return a string giving the current quota usage.
    """
    r = requests.get(quota_url, auth=(os.getenv('PL_API_KEY'), ''))
    if r.ok:
        l = r.json()
        #assert(l[0]['plan']['name'] == 'Education and Research Standard (PlanetScope)')
        return '{:.3f} / {} km²'.format(l[0]['quota_used'], l[0]['quota_sqkm'])
    print('ERROR: {} returned {}'.format(quota_url, r.status_code))


def fname_from_metadata(d):
    """
    Return a string containing the image acquisition date and identifier.

    Args:
        d (dict): dictionary containing a Planet item information
    """
    scene_id = d['id']
    date_str = d['properties']['acquired']
    date = dateutil.parser.parse(date_str).date()
    return '{}_scene_{}'.format(date.isoformat(), scene_id)


def metadata_from_metadata_dict(d):
    """
    Return a dict containing some string-formatted metadata.

    Args:
        d (dict): dictionary containing a Planet item information
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


def download_crop_with_gdal(outfile, asset, ulx, uly, lrx, lry, utm_zone=None,
                            lat_band=None):
    """
    Download a crop defined in UTM coordinates, using gdal_translate.

    Args:
        outfile (string): path to the output file
        asset (dict): dictionary containing the image information
        ulx, uly (floats): x/y UTM coordinates of the upper left (ul) corner
        lrx, lry (floats): x/y UTM coordinates of the lower right (lr) corner
        utm_zone (int): number between 1 and 60 indicating the UTM zone with
            respect to which the UTM coordinates have to be interpreted.
        lat_band (string): letter between C and X indicating the latitude band.
    """
    url = poll_activation(asset)
    if url is not None:
        #if asset.endswith(('_xml', '_rpc')):
        #    os.system('wget {} -O {}'.format(url, outfile))
        #elif asset.startswith('basic'):
        #    os.system('wget {} -O {}'.format(url, outfile))
        #else:
        utils.crop_with_gdal_translate(outfile, url, ulx, uly, lrx, lry,
                                       utm_zone, lat_band)


def get_item_asset_info(item, asset_type, verbose=False):
    """
    Get item asset details if we have download permissions.

    Args:
        item (dict): dictionary containing the item info
        asset_type (string): desired asset

    Return:
        (dict): dictionary containing the item asset information
    """
    allowed_assets = client.get_assets(item).get()
    if asset_type not in allowed_assets:
        if verbose:
            print('WARNING: no permission to get asset "{}" of {}'.format(asset_type,
                                                                          item['_links']['_self']))
            print("\tPermissions for this item are:", item['_permissions'])
        return None
    else:
        return allowed_assets[asset_type]


def request_activation(asset):
    """
    Request the activation of an asset to Planet data API.

    Args:
        asset (dict): dictionary containing the image information
    """
    activation = client.activate(asset)
    r = activation.response.status_code
    if r not in [202, 204]:  # 202: activation started
                             # 204: already active
        print('WARNING: activation of asset {} returned {}'.format(asset, r))


def poll_activation(asset):
    """
    Wait for an asset, requested to Planet data API, to be ready for download.

    Args:
        asset (dict): dictionary containing the asset info

    Return:
        (string): url to the file ready for download
    """
    # refresh the asset info
    r = requests.get(asset['_links']['_self'], auth=(os.environ['PL_API_KEY'], ''))
    if r.ok:
        asset = r.json()
    elif r.status_code == 429:  # rate limit
        time.sleep(1)
        return poll_activation(asset)
    else:
        print('ERROR: got {} error code when requesting {}'.format(r.status_code,
                                                                   asset['_links']['_self']))
        return

    # decide what to do next depending on the asset status
    if asset['status'] == 'active':
        return asset['location']
    elif asset['status'] == 'activating':
        time.sleep(3)
        return poll_activation(asset)
    elif asset['status'] == 'inactive':
        request_activation(asset)
        time.sleep(3)
        return poll_activation(asset)
    else:
        print('ERROR: unknown asset status {}'.format(asset['status']))


def request_clip(item, asset, aoi, active=False):
    """
    Request a clip to Planet clip & ship API.

    Args:
        item (dict): dictionary containing the item info
        asset (dict): dictionary containing the asset info
        aoi (dict): dictionary containing a geojson polygon (e.g. output of
            utils.geojson_geometry_object)
        active (bool): boolean

    Return:
        (dict): dictionary containing the clip info
    """
    if not active:  # wait for the asset to be actived
        poll_activation(asset)

    # request the clip
    d = {
        "aoi": aoi,
        "targets": [
            {
                "item_id": item['id'],
                "item_type": item['properties']['item_type'],
                "asset_type": asset['type']
            }
        ]
    }
    headers = {'content-type': 'application/json'}
    r = requests.post(cas_url,
                      headers=headers, data=json.dumps(d),
                      auth=(os.environ['PL_API_KEY'], ''))
    if r.ok:
        return r.json()
    elif r.status_code == 429:  # rate limit
        time.sleep(1)
        return request_clip(item, asset, aoi, active=True)
    else:
        print('ERROR: got {} error code when requesting {}'.format(r.status_code, d))


def poll_clip(clip_json):
    """
    Wait for a clip, requested to Planet clip & ship API, to be ready.

    Args:
        clip_json (dict): dictionary containing the clip info

    Return:
        (string): url to the zipfile containing the clipped data.
    """
    # refresh the clip info
    clip_request_url = clip_json['_links']['_self']
    r = requests.get(clip_request_url, auth=(os.environ['PL_API_KEY'], ''))
    if r.ok:
        j = r.json()
    elif r.status_code == 429:  # rate limit
        time.sleep(1)
        return poll_clip(clip_json)
    else:
        print('ERROR: got {} error code when requesting {}'.format(r.status_code, clip_request_url))
        return

    # decide what to do next depending on the clip status
    if j['state'] == 'succeeded':
        return j['_links']['results'][0]
    elif j['state'] == 'running':
        time.sleep(3)
        return poll_clip(clip_json)
    else:
        print('ERROR: unknown state "{}" of clip request {}'.format(j['state'],
                                                                    clip_request_url))


def download_clip(clip_info, outpath):
    """
    Download a zipfile from Planet clip & ship endpoint after a clip request.

    Args:
        clip_info (dict): dictionary containing the clip info
        outpath (string): path where to store the downloaded zip file
    """
    url = poll_clip(clip_info)
    utils.download(url, outpath, auth=(os.environ['PL_API_KEY'], ''))


def get_time_series(aoi, start_date=None, end_date=None,
                    item_types=['PSScene3Band'], asset_type='analytic',
                    out_dir='',
                    parallel_downloads=multiprocessing.cpu_count(),
                    clip_and_ship=True):
    """
    Main function: crop and download Planet images.
    """
    # list available images
    items = search_planet.search(aoi, start_date, end_date, item_types=item_types)
    print('Found {} images'.format(len(items)))

    # list the requested asset for each available (and allowed) image
    print('Listing available {} assets...'.format(asset_type), flush=True, end=' ')
    assets = parallel.run_calls(get_item_asset_info, items,
                                extra_args=(asset_type,), pool_type='threads',
                                nb_workers=parallel_downloads, timeout=600)

    # remove 'None' (ie not allowed) assets and corresponding items
    items = [i for (i, a) in zip(items, assets) if a]
    assets = [a for a in assets if a]
    print('Have permissions for {} images'.format(len(items)))

    # activate the allowed assets
    print('Requesting activation of {} images...'.format(len(assets)),
          flush=True, end=' ')
    parallel.run_calls(request_activation, assets, pool_type='threads',
                       nb_workers=parallel_downloads, timeout=600)

    # warn user about quota usage
    n = len(assets)
    if clip_and_ship:
        a = area.area(aoi)
    else:
        a = np.sum(area.area(i['geometry']) for i in items)
    print('Your current quota usage is {}'.format(get_quota()), flush=True)
    print('Downloading these {} images will increase it by {:.3f} km²'.format(n, n*a/1e6),
          flush=True)

    # build filenames
    ext = 'zip' if clip_and_ship else 'tif'
    fnames = [os.path.join(out_dir, '{}.{}'.format(fname_from_metadata(i),
                                                   ext)) for i in items]

    if clip_and_ship:
        print('Requesting clip of {} images...'.format(len(assets)),
              flush=True, end=' ')
        clips = parallel.run_calls(request_clip, list(zip(items, assets)),
                                   extra_args=(aoi,), pool_type='threads',
                                   nb_workers=parallel_downloads, timeout=3600)

        print('Downloading {} clips...'.format(len(clips)), end=' ', flush=True)
        parallel.run_calls(download_clip, list(zip(clips, fnames)),
                           pool_type='threads', nb_workers=parallel_downloads,
                           timeout=3600)

    else:
        # convert aoi coordinates to utm
        ulx, uly, lrx, lry, utm_zone, lat_band = utils.utm_bbx(aoi)

        # download crops with gdal through vsicurl
        utils.mkdir_p(out_dir)
        print('Downloading {} crops...'.format(len(assets)), end=' ')
        parallel.run_calls(download_crop_with_gdal, list(zip(fnames, assets)),
                           extra_args=(ulx, uly, lrx, lry, utm_zone, lat_band),
                           pool_type='threads', nb_workers=parallel_downloads,
                           timeout=300)

        # embed some metadata in the image files
        for f, img in zip(fnames, items):  # embed some metadata as gdal geotiff tags
            if os.path.isfile(f):
                for k, v in metadata_from_metadata_dict(img).items():
                    utils.set_geotif_metadata_item(f, k, v)


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
    parser.add_argument('--parallel-downloads', type=int, default=10,
                        help='max number of parallel crops downloads')
    parser.add_argument('--clip-and-ship', action='store_true', help=('use the '
                                                                      'clip and '
                                                                      'ship API'))
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
                    out_dir=args.outdir,
                    parallel_downloads=args.parallel_downloads,
                    clip_and_ship=args.clip_and_ship)
