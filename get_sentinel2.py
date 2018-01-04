#!/usr/bin/env python
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
import geojson
import shapely.geometry

import utils
import parallel
import search_devseed


# http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com
aws_url = 'http://sentinel-s2-l1c.s3.amazonaws.com'

# list of spectral bands
all_bands = ['TCI', 'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08',
             'B8A', 'B09', 'B10', 'B11', 'B12']

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


def aws_url_from_metadata_dict_backend(d, api='devseed'):
    """
    Build the AWS url of a Sentinel-2 image from it's metadata.
    """
    date, mgrs_id = date_and_mgrs_id_from_metadata_dict(d, api)
    _, utm_code, lat_band, sqid,_ = re.split('(\d+)([a-zA-Z])([a-zA-Z]+)',mgrs_id)
    return '{}/tiles/{}/{}/{}/{}/{}/{}/0/'.format(aws_url, utm_code, lat_band,
                                                  sqid, date.year, date.month,
                                                  date.day)


def aws_url_from_metadata_dict(d, api='devseed', band=None):
    """
    Build the AWS url (including band) of a Sentinel-2 image from it's metadata
    """
    baseurl = aws_url_from_metadata_dict_backend(d, api)
    if band and band in all_bands:
        return '{}{}.jp2'.format(baseurl,band)
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
            orbit = s.group(1)
        else:
            orbit = '000'
        satellite = d['satellite_name']
        satellite = satellite.replace("Sentinel-", "S")  # Sentinel-2B --> S2B
    elif api == 'planet':
        orbit = d['properties']['rel_orbit_number']
        satellite = d['properties']['satellite_id']
        satellite = satellite.replace("Sentinel-", "S")  # Sentinel-2A --> S2A
    elif api == 'scihub':
        orbit = d['int'][1]['content']
        satellite = d['title'][:3]  # S2A_MSIL1C_2018010... --> S2A
    return '{}_{}_orbit_{}_tile_{}'.format(date.date().isoformat(), satellite,
                                           orbit, mgrs_id)


def metadata_from_metadata_dict(d, api='planet'):
    """
    Return a dict containing some string-formatted metadata.
    """
    imaging_date = date_and_mgrs_id_from_metadata_dict(d, api)[0]
    if api == 'planet':
        sun_zenith = 90 - d['properties']['sun_elevation']  # zenith and elevation are complementary
        sun_azimuth = d['properties']['sun_azimuth']
    elif api == 'scihub' or api == 'devseed':
        url = aws_url_from_metadata_dict(d, api)
        r = requests.get('{}metadata.xml'.format(url))
        if r.ok:
            soup = bs4.BeautifulSoup(r.text, 'xml')
            sun_zenith = float(soup.Mean_Sun_Angle.ZENITH_ANGLE.text)
            sun_azimuth = float(soup.Mean_Sun_Angle.AZIMUTH_ANGLE.text)
        else:
            print("WARNING: couldn't retrieve sun azimuth and zenith", url)
            sun_zenith, sun_azimuth = 90, 0
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
                    out_dir='', search_api='devseed',
                    parallel_downloads=multiprocessing.cpu_count()):
    """
    Main function: crop and download a time series of Sentinel-2 images.
    """
    utils.print_elapsed_time.t0 = datetime.datetime.now()

    # list available images
    if search_api == 'devseed':
        images = search_devseed.search(aoi, start_date, end_date,
                                       'Sentinel-2')['results']
    elif search_api == 'scihub':
        import search_scihub
        images = search_scihub.search(aoi, start_date, end_date,
                                      satellite='Sentinel-2')
    elif search_api == 'planet':
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

    # build urls and filenames
    urls = []
    fnames = []
    for img in images:
        url = aws_url_from_metadata_dict(img, search_api)
        name = filename_from_metadata_dict(img, search_api)
        for b in bands:
            urls.append('{}{}.jp2'.format(url, b))
            fnames.append(os.path.join(out_dir, '{}_band_{}.tif'.format(name, b)))

    # convert aoi coordates to utm
    ulx, uly, lrx, lry, utm_zone, lat_band = utils.utm_bbx(aoi)

    # download crops
    utils.mkdir_p(out_dir)
    print('Downloading {} crops ({} images with {} bands)...'.format(len(urls),
                                                                     len(images),
                                                                     len(bands)),
          end=' ')
    parallel.run_calls(utils.crop_with_gdal_translate, list(zip(fnames, urls)),
                       extra_args=(ulx, uly, lrx, lry, utm_zone, lat_band, 'UInt16'),
                       pool_type='threads', nb_workers=parallel_downloads)
    utils.print_elapsed_time()

    # discard images that failed to download
    images = [x for x in images if bands_files_are_valid(x, bands, search_api,
                                                         out_dir)]
    # discard images that are totally covered by clouds
    utils.mkdir_p(os.path.join(out_dir, 'cloudy'))
    urls = [aws_url_from_metadata_dict(img, search_api) for img in images]
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
                        choices=all_bands, metavar='',
                        help=('space separated list of spectral bands to'
                              ' download. Default is B04 (red). Allowed values'
                              ' are {}'.format(', '.join(all_bands))))
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('--api', type=str, choices=['devseed', 'planet', 'scihub'],
                        default='devseed', help='search API')
    parser.add_argument('--parallel-downloads', type=int,
                        default=multiprocessing.cpu_count(),
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
                    bands=args.band, out_dir=args.outdir, search_api=args.api,
                    parallel_downloads=args.parallel_downloads)
