#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic download of Sentinel-1 images.

Copyright (C) 2016-18, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>

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
import os
import zipfile
import argparse
import subprocess
import multiprocessing

import bs4
import requests

from tsd import utils
from tsd import parallel
from tsd import search_scihub
from tsd import s1_metadata_parser


PEPS_URL_SEARCH = 'https://peps.cnes.fr/resto/api/collections'
PEPS_URL_DOWNLOAD = 'https://peps.cnes.fr/resto/collections'
CODEDE_URL = 'https://code-de.org/Sentinel1'


def read_copernicus_credentials_from_environment_variables():
    """
    Read the user Copernicus Open Access Hub credentials.
    """
    try:
        login = os.environ['COPERNICUS_LOGIN']
        password = os.environ['COPERNICUS_PASSWORD']
    except KeyError as e:
        print("The {} module requires the COPERNICUS_LOGIN and".format(os.path.basename(__file__)),
              "COPERNICUS_PASSWORD environment variables to be defined with valid",
              "credentials for https://scihub.copernicus.eu/. Create an account if",
              "you don't have one (it's free) then edit the relevant configuration",
              "files (eg .bashrc) to define these environment variables.")
        raise e
    return login, password


def query_data_hub(output_filename, url, user, password, verbose=False):
    """
    Download a file from the Copernicus data hub or one of its true mirrors.
    """
    verbosity = '--verbose' if verbose else '--no-verbose'  # intermediate verbosity with --quiet
    subprocess.call(['wget',
                     verbosity,
                     '--no-check-certificate',
                     '--auth-no-challenge',
                     '--user=%s' % user,
                     '--password=%s' % password,
                     '--output-document=%s' % output_filename,
                     url])


def download_safe_from_peps(safe_name, out_dir=''):
    """
    """
    try:
        login, password = os.environ['PEPS_LOGIN'], os.environ['PEPS_PASSWORD']
    except KeyError:
        print("Downloading from PEPS requires the PEPS_LOGIN and".format(__file__),
              "PEPS_PASSWORD environment variables to be defined with valid",
              "credentials for https://peps.cnes.fr/. Create an account if",
              "you don't have one (it's free) then edit the relevant configuration",
              "files (eg .bashrc) to define these environment variables.")
        return

    query = '{}/S1/search.atom?identifier={}'.format(PEPS_URL_SEARCH, safe_name)
    r = requests.get(query)

    if not r.ok:
        print('WARNING: request {} failed'.format(query))
        return

    img = bs4.BeautifulSoup(r.text, 'xml').find_all('entry')[0]
    peps_id = img.find('id').text
    url = "{}/S1/{}/download".format(PEPS_URL_DOWNLOAD, peps_id)
    zip_path = os.path.join(out_dir, '{}.SAFE.zip'.format(safe_name))
    cmd = "curl -k --basic -u {}:{} {} -o {}".format(login, password,
                                                     url, zip_path)
    print(cmd)
    os.system(cmd)


def download_sentinel_image(image, out_dir='', mirror='peps'):
    """
    Download a Sentinel image.
    """
    # create output directory
    if out_dir:
        out_dir = os.path.abspath(os.path.expanduser(out_dir))
        os.makedirs(out_dir, exist_ok=True)

    # download zip file
    zip_path = os.path.join(out_dir, '{}.SAFE.zip'.format(image['title']))
    date = image['date']
    if not zipfile.is_zipfile(zip_path) or os.stat(zip_path).st_size == 0:
        if mirror == 'code-de':
            url = '{}/{:04d}/{:02d}/{:02d}/{}.SAFE.zip'.format(CODEDE_URL,
                                                               date.year,
                                                               date.month,
                                                               date.day,
                                                               image['title'])
            if requests.head(url).ok:  # download the file
                subprocess.call(['wget', url])
        elif mirror == 'peps':
            try:
                download_safe_from_peps(image['title'], out_dir=out_dir)
            except Exception:
                print('WARNING: failed request to {}/S1/search.atom?identifier={}'.format(PEPS_URL_SEARCH, image['title']))
        elif mirror in search_scihub.API_URLS:
            url = "{}/odata/v1/Products('{}')/$value".format(search_scihub.API_URLS[mirror],
                                                             image['id'])
            user, password = read_copernicus_credentials_from_environment_variables()
            query_data_hub(zip_path, url, user, password, verbose=True)
        else:
            print('ERROR: unknown mirror {}'.format(mirror))

    return zip_path


def search(aoi, start_date=None, end_date=None, product_type="GRD",
           operational_mode="IW", swath_identifier=None,
           relative_orbit_number=None, api='scihub'):
    """
    Search Sentinel-1 images covering an AOI and timespan using a given API.

    Args:
        aoi (geojson.Polygon): area of interest
        start_date (datetime.datetime): start of the search time range
        end_date (datetime.datetime): end of the search time range
        product_type (str, optional): either 'GRD' or 'SLC'
        api (str, optional): either scihub (default) or planet

    Returns:
        list of image objects
    """
    # list available images
    if api == "scihub":
        from tsd import search_scihub
        images = search_scihub.search(aoi, start_date, end_date,
                                      satellite='Sentinel-1',
                                      product_type=product_type,
                                      operational_mode=operational_mode,
                                      swath_identifier=swath_identifier,
                                      relative_orbit_number=relative_orbit_number,
                                      api="copernicus")
    elif api == 'planet':
        from tsd import search_planet
        images = search_planet.search(aoi, start_date, end_date,
                                      item_types=['Sentinel1L1C'])

    # parse the API metadata
    images = [s1_metadata_parser.Sentinel1Image(img, api) for img in images]

    # sort images by acquisition day, then by mgrs id
    images.sort(key=(lambda k: k.date.date()))

    print('Found {} images'.format(len(images)))
    return images


def download_crops(imgs, aoi, mirror, out_dir, parallel_downloads, timeout=600):
    """
    Download a timeseries of crops with GDAL VSI feature.

    Args:
        imgs (list): list of images
        aoi (geojson.Polygon): area of interest
        mirror (str): either 'aws' or 'gcloud'
        out_dir (str): path where to store the downloaded crops
        parallel_downloads (int): number of parallel downloads
    """
    print('Building {} {} download urls...'.format(len(imgs), mirror), end=' ')
    if mirror == 'scihub':
        parallel.run_calls(s1_metadata_parser.Sentinel1Image.build_scihub_links,
                           imgs, pool_type='threads',
                           nb_workers=parallel_downloads)
    else:
        parallel.run_calls(s1_metadata_parser.Sentinel1Image.build_s3_links,
                           imgs, pool_type='threads',
                           timeout=timeout,
                           nb_workers=parallel_downloads)

    # convert aoi coords from (lon, lat) to UTM
    coords = utils.utm_bbx(aoi,
                           r=60)  # round to multiples of 60m to match Sentinel-2 grid

    crops_args = []
    nb_removed = 0
    for img in imgs:

        if not img.urls[mirror]:  # then it cannot be downloaded
            nb_removed = nb_removed + 1
            continue

        for p in img.polarisations:
            fname = os.path.join(out_dir, '{}_{}.tif'.format(img.filename, p))
            crops_args.append((fname, img.urls[mirror][p], *coords))

    if nb_removed:
        print('Removed {} image(s) with invalid urls'.format(nb_removed))

    out_dir = os.path.abspath(os.path.expanduser(out_dir))
    os.makedirs(out_dir, exist_ok=True)
    print('Downloading {} crops ({} images with 1 or 2 polarisations)...'.format(len(crops_args),
                                                                                 len(imgs) - nb_removed),
          end=' ')
    parallel.run_calls(utils.crop_with_gdalwarp, crops_args,
                       pool_type='processes',
                       nb_workers=parallel_downloads)



def get_time_series(aoi, start_date=None, end_date=None, out_dir='',
                    product_type='GRD', operational_mode='IW',
                    relative_orbit_number=None, swath_identifier=None,
                    search_api='scihub', download_mirror='aws',
                    parallel_downloads=multiprocessing.cpu_count(),
                    timeout=600):
    """
    Main function: download a Sentinel-1 image time serie.
    """
    # list available images
    images = search(aoi, start_date, end_date, product_type=product_type,
                    operational_mode=operational_mode,
                    swath_identifier=swath_identifier,
                    relative_orbit_number=relative_orbit_number,
                    api=search_api)

    if product_type == "GRD":  # then download crops from AWS
        download_miror = "aws"
        download_crops(images, aoi, download_mirror, out_dir,
                       parallel_downloads, timeout=timeout)

    else: # download full images from scihub
        for image in images:
            download_sentinel_image(image, out_dir, download_mirror)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download '
                                                  'of Sentinel-1 images'))
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
    parser.add_argument('--title', help='image title (filename.SAFE)')
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('-t', '--product-type',
                        help='type of image: GRD, SLC, RAW', default='GRD')
    parser.add_argument('-m', '--operational-mode', default='IW',
                        help='acquisiton mode: SM, IW, EW or WV')
    parser.add_argument('--swath-identifier',
                        help='(for S1) subswath id: S1..S6 or IW1..IW3 or EW1..EW5')
    parser.add_argument('--api', default='scihub',
                        help='search API to use: scihub or planet')
    parser.add_argument('--mirror', default='peps',
                        help='download mirror: scihub or aws (GRD only)')
    parser.add_argument('--orbit', type=int,
                        help='relative orbit number, from 1 to 175')
    parser.add_argument('--parallel', type=int, default=multiprocessing.cpu_count(),
                        help='number of parallel downloads')
    parser.add_argument('--timeout', type=int, default=600,
                        help='timeout for images downloads, in seconds')
    args = parser.parse_args()

    if args.geom and (args.lat or args.lon):
        parser.error('--geom and {--lat, --lon} are mutually exclusive')

    if args.title and (args.geom or args.lat or args.lon):
        parser.error('--title, --geom and {--lat, --lon} are mutually exclusive')

    if not args.geom and (not args.lat or not args.lon) and (not args.title):
        parser.error('either --geom, {--lat, --lon} or --title must be defined')

    if args.title:
        download_safe_from_peps(args.title, args.outdir)
    else:
        if args.geom:
            aoi = args.geom
        else:
            aoi = utils.geojson_geometry_object(args.lat, args.lon, args.width,
                                                args.height)
        get_time_series(aoi, start_date=args.start_date, end_date=args.end_date,
                        out_dir=args.outdir, product_type=args.product_type,
                        operational_mode=args.operational_mode,
                        swath_identifier=args.swath_identifier,
                        relative_orbit_number=args.orbit,
                        search_api=args.api, download_mirror=args.mirror,
                        parallel_downloads=args.parallel, timeout=args.timeout)
