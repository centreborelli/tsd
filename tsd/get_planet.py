#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download and crop of Planet images.

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
import time
import json
import argparse
import multiprocessing

import area
import requests
import numpy as np
import dateutil.parser
import rasterio

from tsd import utils
from tsd import parallel
from tsd import search_planet

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

CAS_URL = 'https://api.planet.com/compute/ops/clips/v1'  # clip and ship
QUOTA_URL = 'https://api.planet.com/auth/v1/experimental/public/my/subscriptions'


def get_quota():
    """
    Return a string giving the current quota usage.
    """
    r = requests.get(QUOTA_URL, auth=(os.getenv('PL_API_KEY'), ''))
    if r.ok:
        l = r.json()
        #assert(l[0]['plan']['name'] == 'Education and Research Standard (PlanetScope)')
        return '{:.3f} / {} km²'.format(l[0]['quota_used'], l[0]['quota_sqkm'])
    print('ERROR: {} returned {}'.format(QUOTA_URL, r.status_code))


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


def download_asset(dstfile, asset):
    """
    Download a full asset.
    """
    url = poll_activation(asset)
    if url is not None:
        os.system('wget {} -O {}'.format(url, dstfile))
        #utils.download(url, dstfile)


def download_crop(outfile, asset, aoi, aoi_type):
    """
    Download a crop defined in geographic coordinates using gdal or rasterio.

    Args:
        outfile (string): path to the output file
        asset (dict): dictionary containing the image information
        aoi (geojson.Polygon or 6-tuple): either a (lon, lat) polygon or a UTM
            rectangle (ulx, uly, lrx, lry, utm_zone, lat_band), where
            ulx, uly (floats): x/y UTM coordinates of the upper left (ul) corner
            lrx, lry (floats): x/y UTM coordinates of the lower right (lr) corner
            utm_zone (int): number between 1 and 60 indicating the UTM zone with
                respect to which the UTM coordinates have to be interpreted.
            lat_band (string): letter between C and X indicating the latitude band.
        aoi_type (string): "lonlat_polygon" or "utm_rectangle"
    """
    url = poll_activation(asset)
    if url is not None:
        if aoi_type == "utm_rectangle":
            utils.crop_with_gdal_translate(outfile, url, *aoi)
        elif aoi_type == "lonlat_polygon":
            with rasterio.open(url, 'r') as src:
                rpc_tags = src.tags(ns='RPC')
            crop, x, y = utils.crop_aoi(url, aoi)
            utils.rio_write(outfile, crop,
                            tags={'CROP_OFFSET_XY': '{} {}'.format(x, y)},
                            namespace_tags={'RPC': rpc_tags})


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
    r = requests.post(CAS_URL,
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
                    clip_and_ship=True, no_crop=False, satellite_id=None,
                    search_type='contains', remove_duplicates=True):
    """
    Main function: crop and download Planet images.
    """
    # list available images
    items = search_planet.search(aoi, start_date, end_date,
                                 item_types=item_types,
                                 satellite_id=satellite_id,
                                 search_type=search_type,
                                 remove_duplicates=remove_duplicates)
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
        a = n * area.area(aoi)
    else:
        a = np.sum(area.area(i['geometry']) for i in items)
    print('Your current quota usage is {}'.format(get_quota()), flush=True)
    print('Downloading these {} images will increase it by {:.3f} km²'.format(n, a/1e6),
          flush=True)

    # build filenames
    ext = 'zip' if clip_and_ship else 'tif'
    out_dir = os.path.abspath(os.path.expanduser(out_dir))
    fnames = [os.path.join(out_dir, '{}.{}'.format(fname_from_metadata(i),
                                                   ext)) for i in items]

    if clip_and_ship:
        print('Requesting clip of {} images...'.format(len(assets)),
              flush=True, end=' ')
        clips = parallel.run_calls(request_clip, list(zip(items, assets)),
                                   extra_args=(aoi,), pool_type='threads',
                                   nb_workers=parallel_downloads, timeout=3600)

        # remove clips that were rejected
        ok = [i for i, x in enumerate(clips) if x]
        clips = [clips[i] for i in range(len(clips)) if i in ok]
        fnames = [fnames[i] for i in range(len(fnames)) if i in ok]

        print('Downloading {} clips...'.format(len(clips)), end=' ', flush=True)
        parallel.run_calls(download_clip, list(zip(clips, fnames)),
                           pool_type='threads', nb_workers=parallel_downloads,
                           timeout=3600)

    elif no_crop:  # download full images
        os.makedirs(out_dir, exist_ok=True)
        print('Downloading {} full images...'.format(len(assets)), end=' ')
        parallel.run_calls(download_asset, list(zip(fnames, assets)),
                           pool_type='threads', nb_workers=parallel_downloads,
                           timeout=1200)
    else:
        if asset_type in ['udm', 'visual', 'analytic', 'analytic_dn',
                          'analytic_sr']:
            aoi_type = 'utm_rectangle'
            aoi = utils.utm_bbx(aoi)
        else:
            aoi_type = 'lonlat_polygon'

        # download crops with gdal through vsicurl
        os.makedirs(out_dir, exist_ok=True)
        print('Downloading {} crops...'.format(len(assets)), end=' ')
        parallel.run_calls(download_crop, list(zip(fnames, assets)),
                           extra_args=(aoi, aoi_type),
                           pool_type='threads', nb_workers=parallel_downloads,
                           timeout=300)

        # embed some metadata in the image files
        for f, img in zip(fnames, items):  # embed some metadata as gdal geotiff tags
            if os.path.isfile(f):
                utils.set_geotif_metadata_items(f, metadata_from_metadata_dict(img))


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
    parser.add_argument('--search-type', choices=['contains', 'intersects'],
                        default='contains', help='search type')
    parser.add_argument('--satellite-id', help='satellite identifier, e.g. 0f02')
    parser.add_argument('--keep-duplicates', action='store_true',
                        help='keep all images even when two were acquired within'
                             ' less than 5 minutes (the default behaviour is to'
                             ' discard such duplicates)')
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
    parser.add_argument('--no-crop', action='store_true',
                        help=("don't crop but instead download the whole image files"))
    args = parser.parse_args()

    if args.geom and (args.lat or args.lon):
        parser.error('--geom and {--lat, --lon} are mutually exclusive')

    if args.clip_and_ship and args.no_crop:
        parser.error('--clip-and-ship and --no-crop are mutually exclusive')

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
                    clip_and_ship=args.clip_and_ship,
                    no_crop=args.no_crop,
                    search_type=args.search_type,
                    satellite_id=args.satellite_id,
                    remove_duplicates=~args.keep_duplicates)
