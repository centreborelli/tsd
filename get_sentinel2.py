#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download, crop, registration, filtering, and equalization of
Sentinel-2 images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@m4x.org>
Copyright (C) 2016, Axel Davy <axel.davy@ens.fr>
"""

from __future__ import print_function
import re
import os
import sys
import shutil
import argparse
import dateutil.parser
import requests
import bs4
import geojson
import shapely.geometry

import utils
import parallel
import search_scihub
import search_planet
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from stable.scripts.midway import midway_on_files
from stable.scripts import registration


# http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com
aws_url = 'http://sentinel-s2-l1c.s3.amazonaws.com'


def aws_url_from_metadata_dict(d, api='planet'):
    """
    Build the AWS url of a Sentinel-2 image from it's metadata.
    """
    if api == 'planet':
        mgrs_id = d['properties']['mgrs_grid_id']
        utm_code, lat_band, sqid = mgrs_id[:2], mgrs_id[2], mgrs_id[3:]
        date = dateutil.parser.parse(d['properties']['acquired'])
    elif api == 'scihub':
        # we assume the product name is formatted as in:
        # S2A_MSIL1C_20170410T171301_N0204_R112_T14SQE_20170410T172128
        # the mgrs_id (here 14SQE) is read from the product name in '_T14SQE_'
        mgrs_id = re.findall(r"_T([0-9]{2}[A-Z]{3})_", d['title'])[0]
        utm_code, lat_band, sqid = mgrs_id[:2], mgrs_id[2], mgrs_id[3:]
        date_string = [a['content'] for a in d['date'] if a['name'] == 'beginposition'][0]
        date = dateutil.parser.parse(date_string)

    return '{}/tiles/{}/{}/{}/{}/{}/{}/0/'.format(aws_url, utm_code, lat_band,
                                                  sqid, date.year, date.month,
                                                  date.day)


def filename_from_metadata_dict(d, api='planet'):
    """
    Build a string using the image acquisition date and identifier.
    """
    if api == 'planet':
        mgrs_id = d['properties']['mgrs_grid_id']
        date = dateutil.parser.parse(d['properties']['acquired']).date()
    return '{}_tile_{}'.format(date.isoformat(), mgrs_id)


def metadata_from_metadata_dict(d, api='planet'):
    """
    Return a dict containing some string-formatted metadata.
    """
    if api == 'planet':
        imaging_date = dateutil.parser.parse(d['properties']['acquired'])
        sun_zenith = 90 - d['properties']['sun_elevation']  # zenith and elevation are complementary
        sun_azimuth = d['properties']['sun_azimuth']

    return {
        "IMAGING_DATE": imaging_date.strftime('%Y-%m-%dT%H:%M:%S'),
        "SUN_ZENITH": str(sun_zenith),
        "SUN_AZIMUTH": str(sun_azimuth)
    }


def is_image_cloudy_at_location(image_aws_url, aoi, p=.5):
    """
    Tell if the given area of interest is covered by clouds in a given image.

    The location is considered covered if a fraction larger than p of its surface is
    labeled as clouds in the sentinel-2 gml cloud masks.

    Args:
        image_aws_url: url of the image on AWS
        aoi: geojson object
        p: fraction threshold
    """
    polygons = []
    url = requests.compat.urljoin(image_aws_url, 'qi/MSK_CLOUDS_B00.gml')
    r = requests.get(url)
    if r.ok:
        soup = bs4.BeautifulSoup(r.text, 'xml')
        for polygon in soup.find_all('MaskFeature'):
            if polygon.maskType.text == 'OPAQUE':  # either OPAQUE or CIRRUS
                polygons.append(polygon)
    else:
        print("WARNING: couldn't retrieve cloud mask file", url)
        return False

    clouds = []
    for polygon in polygons:
        try:
            coords = list(map(float, polygon.posList.text.split()))
            points = list(zip(coords[::2], coords[1::2]))
            clouds.append(shapely.geometry.Polygon(points))
        except IndexError:
            pass

    aoi_shape = shapely.geometry.shape(aoi)
    cloudy = shapely.geometry.MultiPolygon(clouds).intersection(aoi_shape)
    return cloudy.area > (p * aoi_shape.area)


def bands_files_are_valid(img, bands, search_api, directory):
    """
    Check if all bands images files are valid.
    """
    name = filename_from_metadata_dict(img, search_api)
    filenames = ['{}_band_{}.tif'.format(name, b) for b in bands]
    paths = [os.path.join(directory, f) for f in filenames]
    return all(utils.is_valid(p) for p in paths)



def get_time_series(aoi, start_date=None, end_date=None, bands=[4],
                    out_dir='', search_api='planet', parallel_downloads=100,
                    register=False, equalize=False, debug=False):
    """
    Main function: download, crop and register a time series of Sentinel-2 images.
    """
    # list available images
    if search_api == 'scihub':
        images = search_scihub.search(aoi, start_date, end_date)['results']
    elif search_api == 'planet':
        images = search_planet.search(aoi, start_date, end_date,
                                      item_types=['Sentinel2L1C'])['features']

        # sort images by acquisition date, then by mgrs id
        images.sort(key=lambda k: (k['properties']['acquired'],
                                   k['properties']['mgrs_grid_id']))

        # remove duplicates (same acquisition day, different mgrs tile id)
        seen = set()
        images = [x for x in images if not (x['properties']['acquired'] in seen
                                            or  # seen.add() returns None
                                            seen.add(x['properties']['acquired']))]
    print('Found {} images'.format(len(images)))

    # convert bands to uppercase strings of length 2: 1 --> '01', '8a' --> '8A'
    bands = [str(b).zfill(2).upper() for b in args.band]

    # build urls and filenames
    urls = []
    fnames = []
    for img in images:
        url = aws_url_from_metadata_dict(img, search_api)
        name = filename_from_metadata_dict(img, search_api)
        for b in bands:
            urls.append('/vsicurl/{}B{}.jp2'.format(url, b))
            fnames.append(os.path.join(out_dir, '{}_band_{}.tif'.format(name, b)))

    # convert aoi coordates to utm
    ulx, uly, lrx, lry, utm_zone = utils.utm_bbx(aoi)

    if register:  # take 100 meters margin in case of forthcoming shift
        ulx -= 50
        uly += 50
        lrx += 50
        lry -= 50

    # download crops
    utils.mkdir_p(out_dir)
    print('Downloading {} crops ({} images with {} bands)...'.format(len(urls),
                                                                     len(images),
                                                                     len(bands)))
    parallel.run_calls(utils.crop_with_gdal_translate, zip(fnames, urls),
                       parallel_downloads, ulx, uly, lrx, lry, utm_zone)

    # discard images that failed to download
    images = [x for x in images if bands_files_are_valid(x, bands, search_api,
                                                         out_dir)]
    # discard images that are totally covered by clouds
    cloudy = []
    for img in images:
        url = aws_url_from_metadata_dict(img, search_api)
        name = filename_from_metadata_dict(img, search_api)
        if is_image_cloudy_at_location(url, aoi):
            cloudy.append(img)
            utils.mkdir_p(os.path.join(out_dir, 'cloudy'))
            for b in bands:
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
        ulx, uly, lrx, lry = utils.utm_bbx(aoi)[:4]
        if debug:  # keep a copy of the cropped images before registration
            bak = os.path.join(out_dir, 'no_registration')
            utils.mkdir_p(bak)
            for bands_fnames in crops:
                for f in bands_fnames:  # crop to remove the margin
                    o = os.path.join(bak, os.path.basename(f))
                    utils.crop_with_gdal_translate(o, f, ulx, uly, lrx, lry,
                                                   utm_zone)

        print('Registering...')
        registration.main(crops, crops, all_pairwise=True)

        for bands_fnames in crops:
            for f in bands_fnames:  # crop to remove the margin
                utils.crop_with_gdal_translate(f, f, ulx, uly, lrx, lry,
                                               utm_zone)

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
                                                  'of Sentinel-2 images'))
    parser.add_argument('--geom', type=utils.valid_geojson,
                        help=('path to geojson file'))
    parser.add_argument('--lat', type=utils.valid_lat,
                        help=('latitude of the center of the rectangle AOI'))
    parser.add_argument('--lon', type=utils.valid_lon,
                        help=('longitude of the center of the rectangle AOI'))
    parser.add_argument('-w', '--width', type=int, help='width of the AOI (m)')
    parser.add_argument('-l', '--height', type=int, help='height of the AOI (m)')
    parser.add_argument('-s', '--start-date', type=utils.valid_date,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_date,
                        help='end date, YYYY-MM-DD')
    parser.add_argument('-b', '--band', nargs='*', default=[4],
                        help=('list of spectral bands, default band 4 (red)'))
    parser.add_argument('-r', '--register', action='store_true',
                        help='register images through time')
    parser.add_argument('-m', '--midway', action='store_true',
                        help='equalize colors with midway')
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-d', '--debug', action='store_true', help=('save '
                                                                    'intermediate '
                                                                    'images'))
    parser.add_argument('--api', type=str, default='planet',
                        help='search API')
    parser.add_argument('--parallel-downloads', type=int, default=100,
                        help='max number of parallel crops downloads')
    args = parser.parse_args()

    if args.geom and (args.lat or args.lon or args.width or args.height):
        parser.error('--geom and {--lat, --lon, -w, -l} are mutually exclusive')

    if not args.geom and (not args.lat or not args.lon):
        parser.error('either --geom or {--lat, --lon} must be defined')

    if args.geom:
        aoi = args.geom
    else:
        aoi = utils.geojson_geometry_object(args.lat, args.lon, args.width,
                                            args.height)
    get_time_series(aoi, start_date=args.start_date, end_date=args.end_date,
                    bands=args.band, register=args.register,
                    equalize=args.midway, out_dir=args.outdir,
                    debug=args.debug, search_api=args.api,
                    parallel_downloads=args.parallel_downloads)
