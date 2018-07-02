#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic crop and download of Sentinel-2 images.

Copyright (C) 2016-17, Carlo de Franchis <carlo.de-franchis@m4x.org>
"""

from __future__ import print_function
import re
import os
import sys
import shutil
import argparse
import multiprocessing
import dateutil.parser
import datetime
import requests
import bs4
import boto3
import botocore
import geojson
import shapely.geometry

import utils
import parallel
import search_devseed


# http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com
AWS_HTTP_URL = 'http://sentinel-s2-l1c.s3.amazonaws.com'
AWS_S3_URL_L1C = 's3://sentinel-s2-l1c'
AWS_S3_URL_L2A = 's3://sentinel-s2-l2a'

# list of spectral bands
ALL_BANDS = ['TCI', 'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08',
             'B8A', 'B09', 'B10', 'B11', 'B12']


def we_can_access_aws_through_s3():
    """
    Test if we can access AWS through s3.
    """
    if 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
        try:
            boto3.session.Session().client('s3').list_objects_v2(Bucket=AWS_S3_URL_L1C[5:])
            return True
        except botocore.exceptions.ClientError:
            pass
    return False


WE_CAN_ACCESS_AWS_THROUGH_S3 = we_can_access_aws_through_s3()


def utm_zone_from_metadata_dict(d, api='devseed'):
    """
    Read the UTM zone number in the dictionary metadata dict.
    """
    if api == 'devseed':
        return d['utm_zone']
    elif api == 'planet':
        mgrs_id = d['properties']['mgrs_grid_id']
        return mgrs_id[:2]
    elif api == 'scihub':
        # we assume the product name is formatted as in:
        # S2A_MSIL1C_20170410T171301_N0204_R112_T14SQE_20170410T172128
        # the mgrs_id (here 14SQE) is read from the product name in '_T14SQE_'
        date_string = [a['content'] for a in d['date'] if a['name'] == 'beginposition'][0]
        date = dateutil.parser.parse(date_string, ignoretz=True)
        if date > datetime.datetime(2016, 12, 6):
            mgrs_id = re.findall(r"_T([0-9]{2}[A-Z]{3})_", d['title'])[0]
            return mgrs_id[:2]
        else:
            #mgrs_id = '14SQE'
            print('ERROR: scihub API cannot be used for Sentinel-2 searches before 2016-12-6')


def date_and_mgrs_id_from_metadata_dict(d, api='devseed'):
    """
    Build a string using the image acquisition date and identifier.
    """
    if api == 'devseed':
        mgrs_id = '{}{}{}'.format(d['utm_zone'], d['latitude_band'],
                                  d['grid_square'])
        date = dateutil.parser.parse(d['timestamp'])
    elif api == 'planet':
        mgrs_id = d['properties']['mgrs_grid_id']
        date = dateutil.parser.parse(d['properties']['acquired'])
    elif api == 'scihub':
        # we assume the product name is formatted as in:
        # S2A_MSIL1C_20170410T171301_N0204_R112_T14SQE_20170410T172128
        # the mgrs_id (here 14SQE) is read from the product name in '_T14SQE_'
        date_string = [a['content'] for a in d['date'] if a['name'] == 'beginposition'][0]
        date = dateutil.parser.parse(date_string, ignoretz=True)
        if date > datetime.datetime(2016, 12, 6):
            mgrs_id = re.findall(r"_T([0-9]{2}[A-Z]{3})_", d['title'])[0]
        else:
            #mgrs_id = '14SQE'
            print('ERROR: scihub API cannot be used for Sentinel-2 searches before 2016-12-6')
    return date, mgrs_id


def title_from_metadata_dict(d, api='devseed'):
    """
    Return the SAFE title from a tile metadata dictionary.
    """
    if api == 'devseed':
        return d['product_id']
    elif api == 'planet':
        return d['id']
    elif api == 'scihub':
        return d['title']


def aws_path_from_metadata_dict(d, api='devseed'):
    """
    Build the AWS path of a Sentinel-2 image from its metadata.
    """
    if 'aws_path' in d:
        return d['aws_path']

    date, mgrs_id = date_and_mgrs_id_from_metadata_dict(d, api)
    utm_code, lat_band, sqid = re.split('(\d+)([a-zA-Z])([a-zA-Z]+)',
                                        mgrs_id)[1:4]
    return 'tiles/{}/{}/{}/{}/{}/{}/0'.format(utm_code, lat_band, sqid,
                                              date.year, date.month, date.day)


def aws_s3_url_from_metadata_dict(d, api='devseed'):
    """
    Build the AWS s3 url of a Sentinel-2 image from its metadata.
    """
    if 'MSIL2A' in title_from_metadata_dict(d, api):
        aws_s3_url = AWS_S3_URL_L2A
    else:
        aws_s3_url = AWS_S3_URL_L1C
    return '{}/{}'.format(aws_s3_url, aws_path_from_metadata_dict(d, api))


def aws_http_url_from_metadata_dict(d, api='devseed', band=None):
    """
    Build the AWS http url of a Sentinel-2 image from its metadata.
    """
    baseurl = '{}/{}'.format(AWS_HTTP_URL, aws_path_from_metadata_dict(d, api))
    if band and band in ALL_BANDS:
        return '{}/{}.jp2'.format(baseurl, band)
    else:
        return baseurl


def filename_from_metadata_dict(d, api='devseed'):
    """
    Build a string using the image acquisition date and identifier.
    """
    date, mgrs_id = date_and_mgrs_id_from_metadata_dict(d, api)
    if api == 'devseed':
        s = re.search('_R([0-9]{3})_', d['product_id'])
        if s:
            orbit = int(s.group(1))
        else:
            orbit = 0
        satellite = d['satellite_name']
        satellite = satellite.replace("Sentinel-", "S")  # Sentinel-2B --> S2B
    elif api == 'planet':
        orbit = d['properties']['rel_orbit_number']
        satellite = d['properties']['satellite_id']
        satellite = satellite.replace("Sentinel-", "S")  # Sentinel-2A --> S2A
    elif api == 'scihub':
        orbit = int(d['int'][1]['content'])
        satellite = d['title'][:3]  # S2A_MSIL1C_2018010... --> S2A
    return '{}_{}_orbit_{:03d}_tile_{}'.format(date.date().isoformat(),
                                               satellite, orbit, mgrs_id)


def sun_angles(img, api='planet'):
    """
    Return the azimuth and elevation sun angles.

    Args:
        img:
        api:

    Return:
        tuple of length 2 (azimuth, elevation)
    """
    if api == 'planet':
        p = img['properties']
        return p['sun_azimuth'], p['sun_elevation']
    elif api in ['scihub', 'devseed']:
        url = aws_http_url_from_metadata_dict(d, api)
        r = requests.get('{}/metadata.xml'.format(url))
        if r.ok:
            soup = bs4.BeautifulSoup(r.text, 'xml')
            sun_azimuth = float(soup.Mean_Sun_Angle.AZIMUTH_ANGLE.text)
            sun_zenith = float(soup.Mean_Sun_Angle.ZENITH_ANGLE.text)
        else:
            print("WARNING: couldn't retrieve sun azimuth and zenith", url)
            sun_zenith, sun_azimuth = 90, 0
        return sun_azimuth, 90 - sun_zenith  # elevation and zenith are complementary


def band_resolution(b):
    """
    """
    if b in ['B02', 'B03', 'B04', 'B08', 'TCI']:
        return 10
    elif b in ['B05', 'B06', 'B07', 'B8a', 'B11', 'B12']:
        return 20
    elif b in ['B01', 'B09', 'B10']:
        return 60
    else:
        print('ERROR: {} is not in {}'.format(b, ALL_BANDS))


def format_metadata_dict(d):
    """
    Return a copy of the input dict with all values converted to strings.
    """
    return {k: str(d[k]) for k in d}


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
    url = '{}/qi/MSK_CLOUDS_B00.gml'.format(image_aws_url)
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
    try:
        cloudy = shapely.geometry.MultiPolygon(clouds).intersection(aoi_shape)
        return cloudy.area > (p * aoi_shape.area)
    except shapely.geos.TopologicalError:
        return False


def bands_files_are_valid(img, bands, search_api, directory):
    """
    Check if all bands images files are valid.
    """
    name = filename_from_metadata_dict(img, search_api)
    filenames = ['{}_band_{}.tif'.format(name, b) for b in bands]
    paths = [os.path.join(directory, f) for f in filenames]
    return all(utils.is_valid(p) for p in paths)


def get_time_series(aoi, start_date=None, end_date=None, bands=['B04'],
                    out_dir='', search_api='devseed', product_type=None,
                    parallel_downloads=multiprocessing.cpu_count()):
    """
    Main function: crop and download a time series of Sentinel-2 images.
    """
    utils.print_elapsed_time.t0 = datetime.datetime.now()

    # list available images
    if search_api == 'devseed':
        if product_type is not None:
            print("WARNING: product_type option is available only with search_api='scihub'")
        images = search_devseed.search(aoi, start_date, end_date,
                                       'Sentinel-2')['results']
    elif search_api == 'scihub':
        import search_scihub
        if product_type is not None:
            product_type = 'S2MSI{}'.format(product_type[1:])
        images = search_scihub.search(aoi, start_date, end_date,
                                      satellite='Sentinel-2', product_type=product_type)
    elif search_api == 'planet':
        if product_type is not None:
            print("WARNING: product_type option is available only with search_api='scihub'")
        import search_planet
        images = search_planet.search(aoi, start_date, end_date,
                                      item_types=['Sentinel2L1C'])

    # sort images by acquisition date, then by mgrs id
    images.sort(key=lambda k: date_and_mgrs_id_from_metadata_dict(k, search_api))

    # remove duplicates (same acquisition day, different mgrs tile id)
    seen = set()
    images = [x for x in images if not (date_and_mgrs_id_from_metadata_dict(x, search_api)[0] in seen
                                        or  # seen.add() returns None
                                        seen.add(date_and_mgrs_id_from_metadata_dict(x, search_api)[0]))]
    print('Found {} images'.format(len(images)))
    utils.print_elapsed_time()

    # choose wether to use http or s3
    if WE_CAN_ACCESS_AWS_THROUGH_S3:
        aws_url_from_metadata_dict = aws_s3_url_from_metadata_dict
    else:
        aws_url_from_metadata_dict = aws_http_url_from_metadata_dict

    # build urls, filenames and crops coordinates
    crops_args = []
    for img in images:
        url_base = aws_url_from_metadata_dict(img, search_api)
        name = filename_from_metadata_dict(img, search_api)
        coords = utils.utm_bbx(aoi,  # convert aoi coordates to utm
                               utm_zone=int(utm_zone_from_metadata_dict(img, search_api)),
                               r=60)  # round to multiples of 60 (B01 resolution)
        for b in bands:
            fname = os.path.join(out_dir, '{}_band_{}.tif'.format(name, b))
            if 'MSIL2A' in title_from_metadata_dict(img, search_api):
                url = '{}/R{}m/{}.jp2'.format(url_base, band_resolution(b), b)
            else:
                url = '{}/{}.jp2'.format(url_base, b)
            crops_args.append((fname, url, *coords))

    # download crops
    utils.mkdir_p(out_dir)
    print('Downloading {} crops ({} images with {} bands)...'.format(len(crops_args),
                                                                     len(images),
                                                                     len(bands)),
          end=' ')
    parallel.run_calls(utils.crop_with_gdal_translate, crops_args,
                       extra_args=('UInt16',), pool_type='threads',
                       nb_workers=parallel_downloads)
    utils.print_elapsed_time()

    # discard images that failed to download
    images = [x for x in images if bands_files_are_valid(x, bands, search_api,
                                                         out_dir)]
    # discard images that are totally covered by clouds
    utils.mkdir_p(os.path.join(out_dir, 'cloudy'))
    urls = [aws_http_url_from_metadata_dict(img, search_api) for img in images]
    print('Reading {} cloud masks...'.format(len(urls)), end=' ')
    cloudy = parallel.run_calls(is_image_cloudy_at_location, urls,
                                extra_args=(utils.geojson_lonlat_to_utm(aoi),),
                                pool_type='threads',
                                nb_workers=parallel_downloads, verbose=True)
    for img, cloud in zip(images, cloudy):
        name = filename_from_metadata_dict(img, search_api)
        if cloud:
            for b in bands:
                f = '{}_band_{}.tif'.format(name, b)
                shutil.move(os.path.join(out_dir, f),
                            os.path.join(out_dir, 'cloudy', f))
    print('{} cloudy images out of {}'.format(sum(cloudy), len(images)))
    images = [i for i, c in zip(images, cloudy) if not c]
    utils.print_elapsed_time()

    # embed some metadata in the remaining image files
    print('Embedding metadata in geotiff headers...')
    for img in images:
        name = filename_from_metadata_dict(img, search_api)
        d = format_metadata_dict(img)
        for b in bands:  # embed some metadata as gdal geotiff tags
            f = os.path.join(out_dir, '{}_band_{}.tif'.format(name, b))
            utils.set_geotif_metadata(f, metadata=d)
    utils.print_elapsed_time()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Sentinel-2 images'))
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
    parser.add_argument('-b', '--band', nargs='*', default=['B04'],
                        choices=ALL_BANDS + ['all'], metavar='',
                        help=('space separated list of spectral bands to'
                              ' download. Default is B04 (red). Allowed values'
                              ' are {}'.format(', '.join(ALL_BANDS))))
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('--api', type=str, choices=['devseed', 'planet', 'scihub'],
                        default='devseed', help='search API')
    parser.add_argument('--product-type', choices=['L1C', 'L2A'], help='type of image')
    parser.add_argument('--parallel-downloads', type=int,
                        default=multiprocessing.cpu_count(),
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
                    bands=args.band, out_dir=args.outdir, search_api=args.api,
                    product_type=args.product_type,
                    parallel_downloads=args.parallel_downloads)
