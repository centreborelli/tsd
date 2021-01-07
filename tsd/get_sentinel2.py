#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic crop and download of Sentinel-2 images.

Copyright (C) 2016-2020, Carlo de Franchis <carlo.de-franchis@m4x.org>

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
import shutil
import argparse
import multiprocessing
import datetime
import requests
import bs4
import boto3
import shapely.geometry

from tsd import utils
from tsd import parallel
from tsd import s2_metadata_parser


def search(aoi=None, start_date=None, end_date=None, product_type="L2A",
           tile_id=None, title=None, relative_orbit_number=None,
           api='stac', search_type='contains',
           unique_mgrs_tile_per_orbit=True):
    """
    Search Sentinel-2 images covering an AOI and timespan using a given API.

    Args:
        aoi (geojson.Polygon): area of interest
        start_date (datetime.datetime): start of the search time range
        end_date (datetime.datetime): end of the search time range
        tile_id (str): MGRS tile identifier, e.g. "31TCJ"
        title (str): product title, e.g. "S2A_MSIL1C_20160105T143732_N0201_R096_T19KGT_20160105T143758"
        relative_orbit_number (int): relative orbit number, from 1 to 143
        product_type (str, optional): either "L1C" or "L2A" (default). Ignored
            with planet and gcloud APIs.
        api (str, optional): either stac (default), scihub, planet or gcloud
        search_type (str): either "contains" or "intersects"
        unique_mgrs_tile_per_orbit (bool): if True, only one MGRS tile per
            orbit is considered. The selected MGRS tile is the first in
            alphabetical order. This is useful to remove duplicates when the
            input AOI intersects several MGRS tiles.

    Returns:
        list of image objects
    """
    # list available images
    if api == 'scihub':
        from tsd import search_scihub
        if product_type is not None:
            product_type = 'S2MSI{}'.format(product_type[1:])
        images = search_scihub.search(aoi, start_date, end_date,
                                      satellite="Sentinel-2",
                                      product_type=product_type,
                                      relative_orbit_number=relative_orbit_number,
                                      tile_id=tile_id,
                                      title=title,
                                      search_type=search_type)
    elif api == 'stac':
        from tsd import search_stac
        images = search_stac.search(aoi, start_date, end_date,
                                    satellite="Sentinel-2",
                                    product_type=product_type)
    elif api == 'planet':
        from tsd import search_planet
        images = search_planet.search(aoi, start_date, end_date,
                                      item_types=['Sentinel2L1C'])
    elif api == 'gcloud':
        from tsd import search_gcloud
        images = search_gcloud.search(aoi, start_date, end_date)

    # parse the API metadata
    images = [s2_metadata_parser.Sentinel2Image(img, api) for img in images]

    # sort images by date, relative_orbit, mgrs_id
    images.sort(key=(lambda k: (k.date.date(), k.relative_orbit, k.mgrs_id)))

    # remove duplicates (same pair (date, relative_orbit) but different mgrs_id)
    if unique_mgrs_tile_per_orbit:
        seen = set()
        unique_images = []
        for img in images:
            if (img.date.date(), img.relative_orbit) not in seen:
                seen.add((img.date.date(), img.relative_orbit))
                unique_images.append(img)
        images = unique_images

    print('Found {} images'.format(len(images)))
    return images


def download(imgs, bands, aoi, mirror, out_dir, parallel_downloads, no_crop=False):
    """
    Download a timeseries of crops with GDAL VSI feature.

    Args:
        imgs (list): list of images
        bands (list): list of bands
        aoi (geojson.Polygon): area of interest
        mirror (str): either 'aws' or 'gcloud'
        out_dir (str): path where to store the downloaded crops
        parallel_downloads (int): number of parallel downloads
        no_crop (bool): don't crop but instead download the original JP2 files
    """
    if mirror == "gcloud":
        parallel.run_calls(s2_metadata_parser.Sentinel2Image.build_gs_links,
                           imgs, pool_type='threads',
                           verbose=False,
                           nb_workers=parallel_downloads)
    elif mirror == "aws":
        parallel.run_calls(s2_metadata_parser.Sentinel2Image.build_s3_links,
                           imgs, pool_type='threads',
                           verbose=False,
                           nb_workers=parallel_downloads)
    else:
        raise ValueError(f"Unknown mirror {mirror}")

    crops_args = []
    nb_removed = 0
    for img in imgs:

        if not img.urls[mirror]:  # then it cannot be downloaded
            nb_removed = nb_removed + 1
            continue

        # convert aoi coords from (lon, lat) to UTM in the zone of the image
        coords = ()
        if aoi is not None:
            coords = utils.utm_bbx(aoi, epsg=int(img.epsg),
                                   r=60)  # round to multiples of 60 (B01 resolution)

        for b in bands:
            fname = os.path.join(out_dir, '{}_band_{}.tif'.format(img.filename, b))
            crops_args.append((fname, img.urls[mirror][b], *coords))

    if nb_removed:
        print('Removed {} image(s) with invalid urls'.format(nb_removed))

    out_dir = os.path.abspath(os.path.expanduser(out_dir))
    os.makedirs(out_dir, exist_ok=True)
    print('Downloading {} crops ({} images with {} bands)...'.format(len(crops_args),
                                                                     len(imgs) - nb_removed,
                                                                     len(bands)))

    if no_crop or (aoi is None):  # download original files
        for fname, url, *_ in crops_args:
            ext = url.split(".")[-1]  # jp2, TIF, ...
            utils.download(url, fname.replace(".tif", f".{ext}"))

    else:  # download crops
        parallel.run_calls(utils.rasterio_geo_crop, crops_args,
                           pool_type='threads',
                           nb_workers=parallel_downloads)


def bands_files_are_valid(img, bands, d):
    """
    Check if all bands images files are valid.
    """
    return all(utils.is_valid(os.path.join(d, f"{img.filename}_band_{b}.tif")) for b in bands)


def is_image_cloudy(img, aoi, mirror, p=0.5):
    """
    Tell if the given area of interest is covered by clouds in a given image.

    The location is considered covered if a fraction larger than p of its surface is
    labeled as clouds in the sentinel-2 gml cloud masks.

    Args:
        img (image object): Sentinel-2 image metadata
        aoi (geojson.Polygon): area of interest
        mirror (string): either 'gcloud' or 'aws'
        p (float): fraction threshold

    Return:
        boolean (True if the image is cloudy, False otherwise)
    """
    url = img.urls[mirror]['cloud_mask']

    if mirror == 'gcloud':
        gml_content = requests.get(url).text
    else:
        bucket, *key = url.replace('s3://', '').split('/')
        f = boto3.client('s3').get_object(Bucket=bucket, Key='/'.join(key),
                                          RequestPayer='requester')['Body']
        gml_content = f.read()

    clouds = []
    soup = bs4.BeautifulSoup(gml_content, 'xml')
    for polygon in soup.find_all('MaskFeature'):
        if polygon.maskType.text == 'OPAQUE':  # either OPAQUE or CIRRUS
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


def read_cloud_masks(aoi, imgs, bands, mirror, parallel_downloads, p=0.5,
                     out_dir=''):
    """
    Read Sentinel-2 GML cloud masks and intersects them with the input aoi.

    Args:
        aoi (geojson.Polygon): area of interest
        imgs (list): list of images
        bands (list): list of bands
        mirror (str): either 'aws' or 'gcloud'
        parallel_downloads (int): number of parallel gml files downloads
        p (float): cloud area threshold above which our aoi is said to be
            'cloudy' in the current image
    """
    print('Reading {} cloud masks...'.format(len(imgs)), end=' ')
    cloudy = parallel.run_calls(is_image_cloudy, imgs,
                                extra_args=(utils.geojson_lonlat_to_utm(aoi), mirror, p),
                                pool_type='threads',
                                nb_workers=parallel_downloads, verbose=True)
    print('{} cloudy images out of {}'.format(sum(cloudy), len(imgs)))

    for img, cloud in zip(imgs, cloudy):
        if cloud:
            out_dir = os.path.abspath(os.path.expanduser(out_dir))
            os.makedirs(os.path.join(out_dir, 'cloudy'), exist_ok=True)
            for b in bands:
                f = '{}_band_{}.tif'.format(img.filename, b)
                shutil.move(os.path.join(out_dir, f),
                            os.path.join(out_dir, 'cloudy', f))


def get_time_series(aoi=None, start_date=None, end_date=None, bands=["B04"],
                    tile_id=None, title=None, relative_orbit_number=None,
                    out_dir="", api="stac", mirror="aws",
                    product_type="L2A", cloud_masks=False,
                    parallel_downloads=multiprocessing.cpu_count(),
                    satellite_angles=False, no_crop=False):
    """
    Main function: crop and download a time series of Sentinel-2 images.

    Args:
        aoi (geojson.Polygon): area of interest
        start_date (datetime.datetime, optional): start of the time range
        end_date (datetime.datetime, optional): end of the time range
        bands (list, optional): list of bands
        tile_id (str): MGRS tile identifier, e.g. "31TCJ"
        title (str): product title, e.g. "S2A_MSIL1C_20160105T143732_N0201_R096_T19KGT_20160105T143758"
        relative_orbit_number (int): relative orbit number, from 1 to 143
        out_dir (str, optional): path where to store the downloaded crops
        api (str, optional): either stac (default), scihub, planet or gcloud
        mirror (str, optional): either 'aws' (default) or 'gcloud'
        product_type (str, optional): either 'L1C' or 'L2A' (default)
        cloud_masks (bool, optional): if True, cloud masks are downloaded and
            cloudy images are discarded
        parallel_downloads (int): number of parallel gml files downloads
        satellite_angles (bool): whether or not to download satellite zenith
            and azimuth angles and include them in metadata
        no_crop (bool): if True, download original JP2 files rather than crops
    """
    # list available images
    images = search(aoi, start_date, end_date,
                    relative_orbit_number=relative_orbit_number,
                    tile_id=tile_id,
                    title=title,
                    product_type=product_type,
                    api=api)

    # download crops
    download(images, bands, aoi, mirror, out_dir, parallel_downloads, no_crop)

    # discard images that failed to download
    images = [i for i in images if bands_files_are_valid(i, bands, out_dir)]

    if satellite_angles:  # retrieve satellite elevation and azimuth angles
        for img in images:
            img.get_satellite_angles()

    # embed all metadata as GeoTIFF tags in the image files
    for img in images:
        img['downloaded_by'] = 'TSD on {}'.format(datetime.datetime.now().isoformat())

        for b in bands:
            filepath = os.path.join(out_dir, '{}_band_{}.tif'.format(img.filename, b))
            utils.set_geotif_metadata_items(filepath, img)

    if cloud_masks:  # discard images that are totally covered by clouds
        read_cloud_masks(aoi, images, bands, mirror, parallel_downloads,
                         out_dir=out_dir)


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
    parser.add_argument('-b', '--band', nargs='*', default=['B04'], metavar='',
                        choices=s2_metadata_parser.BANDS_L2A + ['all'],
                        help=('space separated list of spectral bands to'
                              ' download. Default is B04 (red). Allowed values'
                              ' are {}'.format(', '.join(s2_metadata_parser.BANDS_L2A))))
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('--api', type=str, choices=['scihub', 'stac', 'planet', 'gcloud'],
                        default='stac', help='search API')
    parser.add_argument('--mirror', type=str, choices=['aws', 'gcloud'],
                        default='aws', help='download mirror')
    parser.add_argument('--product-type', choices=['L1C', 'L2A'],
                        default="L2A", help='processing level')
    parser.add_argument('--tile-id',
                        help='MGRS tile identifier, e.g. 31TCJ')
    parser.add_argument('--title',
                        help='Product title (e.g. S2A_MSIL1C_20160105T143732_N0201_R096_T19KGT_20160105T143758)')
    parser.add_argument('--relative-orbit-number', type=int,
                        help='Relative orbit number, from 1 to 143')
    parser.add_argument('--parallel-downloads', type=int,
                        default=multiprocessing.cpu_count(),
                        help='max number of parallel crops downloads')
    parser.add_argument('--cloud-masks', action='store_true',
                        help=('download cloud masks crops from provided GML files'))
    parser.add_argument('--satellite-angles', action='store_true',
                        help=('retrieve satellite zenith and azimuth angles'
                              ' and include them in tiff metadata'))
    parser.add_argument('--no-crop', action='store_true',
                        help=("don't crop but instead download the original JP2 files"))

    args = parser.parse_args()

    if 'all' in args.band:
        args.band = s2_metadata_parser.BANDS_L2A if args.product_type == "L2A" else s2_metadata_parser.BANDS_L1C

    if args.lat is not None and args.lon is not None:
        args.geom = utils.geojson_geometry_object(args.lat, args.lon,
                                                  args.width, args.height)

    get_time_series(args.geom,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    bands=args.band,
                    tile_id=args.tile_id,
                    title=args.title,
                    relative_orbit_number=args.relative_orbit_number,
                    out_dir=args.outdir,
                    api=args.api,
                    mirror=args.mirror,
                    no_crop=args.no_crop,
                    product_type=args.product_type,
                    cloud_masks=args.cloud_masks,
                    parallel_downloads=args.parallel_downloads,
                    satellite_angles=args.satellite_angles)
