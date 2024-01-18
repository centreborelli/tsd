#!/usr/bin/env python3
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Automatic crop and download of Sentinel-3 images.

Copyright (C) 2022, Thibaud Ehret <thibaud.ehret@ens-cachan.fr>
Copyright (C) 2022, Carlo de Franchis <carlo.de-franchis@m4x.org>

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
import rasterio

from tsd import utils
from tsd import parallel
from tsd import s3_metadata_parser


def search(aoi=None, start_date=None, end_date=None, product_type="SL_1_RBT___",
           title=None, orbit_direction=None, tml=None, clouds=False,
           api='cdse', search_type='contains'):
    """
    Search Sentinel-3 images covering an AOI and timespan using a given API.

    Args:
        aoi (geojson.Polygon): area of interest
        start_date (datetime.datetime): start of the search time range
        end_date (datetime.datetime): end of the search time range
        title (str): product title, e.g. "S3B_SL_1_RBT____20221005T193828_20221005T194128_20221006T130558_0179_071_170_4320_PS2_O_NT_004_"
        orbit_direction (str): either None (all, default), descending or ascending
        product_type (str, optional): either "SL_1_RBT___" (default) or "SL_2_FRP___".
        api (str, optional): only cdse (default) is supported right now
        search_type (str): either "contains" or "intersects"

    Returns:
        list of image objects
    """
    assert api in ['cdse'], "Invalid API"

    # list available images
    if api == 'cdse':
        from tsd import search_scihub
        images = search_scihub.search(aoi=aoi,
                                      start_date=start_date,
                                      end_date=end_date,
                                      satellite="Sentinel-3",
                                      product_type=product_type,
                                      orbit_direction=orbit_direction,
                                      tml=tml,
                                      title=title,
                                      search_type=search_type)
        images.sort(key=(lambda k: (k['title'])))

        # Retrieve a cloud index at the same time
        if clouds:
            imagesc = search_scihub.search(aoi=aoi,
                                           start_date=start_date,
                                           end_date=end_date,
                                           satellite="Sentinel-3",
                                           product_type='SL_2_LST___',
                                           orbit_direction=orbit_direction,
                                           tml=tml,
                                           title=title,
                                           search_type=search_type)
            imagesc.sort(key=(lambda k: (k['title'])))
            assert len(images) == len(imagesc), "The assumption that L1 products match L2 products is wrong, please report it"

    # parse the API metadata
    images = [s3_metadata_parser.Sentinel3Image(img, cloud, api) for img, cloud in zip(images, imagesc)]

    # sort images by acquisition date
    images.sort(key=(lambda k: (k.date.date())))

    print('Found {} images'.format(len(images)))
    return images


def download(imgs, bands, aoi, mirror, out_dir, parallel_downloads, no_crop=False, timeout=60):
    """
    Download a timeseries of crops with GDAL VSI feature.

    Args:
        imgs (list): list of images
        bands (list): list of bands
        aoi (geojson.Polygon): area of interest
        mirror (str): 'aws'
        out_dir (str): path where to store the downloaded crops
        parallel_downloads (int): number of parallel downloads
        no_crop (bool): don't crop but instead download the original TIFF files
    """

    if mirror == "aws":
        parallel.run_calls(s3_metadata_parser.Sentinel3Image.build_s3_links,
                           imgs, pool_type='threads',
                           verbose=False,
                           nb_workers=parallel_downloads,
                           timeout=timeout)
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
            coords = utils.utm_bbx(aoi, epsg=4326,
                                   r=500)  # round to multiples of 500 (S3 resolution)

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
        parallel.run_calls(utils.rasterio_geo_crop, crops_args, kwd_args={'aws_unsigned':True},
                           pool_type='threads',
                           nb_workers=parallel_downloads)


def bands_files_are_valid(img, bands, d):
    """
    Check if all bands images files are valid.
    """
    return all(utils.is_valid(os.path.join(d, f"{img.filename}_band_{b}.tif")) for b in bands)

def is_image_cloudy(img, aoi, mirror, p=0.5, out_dir=''):
    """
    Tell if the given area of interest is covered by clouds in a given image and save
    the corresponding cloud mask.

    The location is considered covered if a fraction larger than p of its surface is
    labeled as clouds in the sentinel-3 cloud masks (land surface product).

    Args:
        img (image object): Sentinel-3 image metadata
        aoi (geojson.Polygon): area of interest
        mirror (string): 'aws'
        p (float): fraction threshold

    Return:
        boolean (True if the image is cloudy, False otherwise)
    """
    url = img.urls[mirror]['cloud_mask']

    coords = utils.utm_bbx(aoi, epsg=4326,
                               r=500)  # round to multiples of 500 (S3 resolution)
    fname = os.path.join(out_dir, '{}_cloud.tif'.format(img.filename))

    utils.rasterio_geo_crop(fname, url, *coords, aws_unsigned=True)

    try:
        with rasterio.open(fname) as src:
            clouds = src.read()
            profile = src.profile
    except rasterio.errors.RasterioIOError:
        print("WARNING: download of {} failed".format(fname))
        return 0

    # Transform the provided cloud mask into a binary cloud mask
    clouds = (clouds > 0).astype('uint8')
    height, width = clouds.shape[1:]

    profile.update({"driver": "GTiff",
                    "compress": "deflate",
                    "height": height,
                    "width": width,
                    "nodata": 1,
                    "dtype": 'uint8'})

    with rasterio.open(fname, "w", **profile) as out:
        out.write(clouds)

    return clouds.sum() > (p * height * width)

def read_cloud_masks(aoi, imgs, bands, mirror, parallel_downloads, p=0.5,
                     out_dir=''):
    """
    Read Sentinel-3 cloud masks and intersects them with the input aoi.

    Args:
        aoi (geojson.Polygon): area of interest
        imgs (list): list of images
        bands (list): list of bands
        mirror (str): Only 'aws' (default) is supported right now
        parallel_downloads (int): number of parallel TIFF files downloads
        p (float): cloud area threshold above which our aoi is said to be
            'cloudy' in the current image
    """
    print('Reading {} cloud masks...'.format(len(imgs)), end=' ')
    cloudy = parallel.run_calls(is_image_cloudy, imgs,
                                extra_args=(aoi, mirror, p, out_dir),
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
            f = '{}_cloud.tif'.format(img.filename)
            shutil.move(os.path.join(out_dir, f),
                        os.path.join(out_dir, 'cloudy', f))


def get_time_series(aoi=None, start_date=None, end_date=None, bands=["all"],
                    tile_id=None, title=None, orbit_direction=None, tml=None,
                    out_dir="", api="cdse", mirror="aws",
                    product_type="SL_1_RBT___", cloud_masks=False,
                    parallel_downloads=multiprocessing.cpu_count(),
                    satellite_angles=False, no_crop=False, timeout=60):
    """
    Main function: crop and download a time series of Sentinel-2 images.

    Args:
        aoi (geojson.Polygon): area of interest
        start_date (datetime.datetime, optional): start of the time range
        end_date (datetime.datetime, optional): end of the time range
        bands (list, optional): list of bands
        title (str): product title, e.g. "S3B_SL_1_RBT____20221005T193828_20221005T194128_20221006T130558_0179_071_170_4320_PS2_O_NT_004_"
        orbit_direction (str): either all (default), descending or ascending
        out_dir (str, optional): path where to store the downloaded crops
        api (str, optional): only cdse (default) is supported right now
        mirror (str, optional): only 'aws' (default) is supported right now
        product_type (str, optional): only "SL_1_RBT___" (default) is supported right now
        cloud_masks (bool, optional): if True, cloud masks are downloaded and
            cloudy images are discarded
        parallel_downloads (int): number of parallel gml files downloads
        satellite_angles (bool): whether or not to download satellite zenith
            and azimuth angles and include them in metadata
        no_crop (bool): if True, download original TIFF files rather than crops
        timeout (scalar, optional): timeout for images download, in seconds
    """
    # list available images
    images = search(aoi, start_date, end_date,
                    orbit_direction=orbit_direction,
                    title=title, clouds=cloud_masks,
                    product_type=product_type, tml=tml,
                    api=api) # TODO cloud

    # download crops
    download(images, bands, aoi, mirror, out_dir, parallel_downloads, no_crop, timeout)

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
                                                  'of Sentinel-3 images'))
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
    parser.add_argument('-b', '--band', nargs='*', default=['S1'], metavar='',
                        choices=list(set(s3_metadata_parser.BANDS_L1 + ['all'])),
                        help=('space separated list of spectral bands to'
                              ' download. Default is S1. Allowed values'
                              ' are {}'.format(', '.join(list(set(s3_metadata_parser.BANDS_L1))))))
    parser.add_argument('-o', '--outdir', type=str, help=('path to save the '
                                                          'images'), default='')
    parser.add_argument('--api', type=str, choices=['cdse'],
                        default='cdse', help='search API')
    parser.add_argument('--mirror', type=str, choices=['aws'],
                        default='aws', help='download mirror')
    parser.add_argument('--product-type', choices=['SL_1_RBT___'],
                        default="SL_1_RBT___", help='processing level')
    parser.add_argument('--title',
                        help='Product title (e.g. \
                        S3B_SL_1_RBT____20221005T193828_20221005T194128_20221006T130558_0179_071_170_4320_PS2_O_NT_004)')
    parser.add_argument('--orbit-direction', type=str, choices=['all', 'descending', 'ascending'], default='all',
                        help='Orbit direction')
    parser.add_argument('--timeline', type=str, choices=['all', 'NRT', 'NTC'],
                        default='NTC', help='Product timeline')
    parser.add_argument('--parallel-downloads', type=int,
                        default=multiprocessing.cpu_count(),
                        help='max number of parallel crops downloads')
    parser.add_argument('--cloud-masks', action='store_true',
                        help=('download cloud masks crops from provided TIFF files'))
    ## Not yet supported
    #parser.add_argument('--satellite-angles', action='store_true',
    #                    help=('retrieve satellite zenith and azimuth angles'
    #                          ' and include them in tiff metadata'))
    parser.add_argument('--no-crop', action='store_true',
                        help=("don't crop but instead download the original TIFF files"))
    parser.add_argument('--timeout', type=int, default=60,
                        help='timeout for images downloads, in seconds')

    args = parser.parse_args()

    if 'all' in args.band:
        args.band = s3_metadata_parser.BANDS_L1

    if 'all' == args.orbit_direction:
        args.orbit_direction = None

    assert not args.cloud_masks or args.timeline == 'NTC', "Cloud maps are only copmpatible with the NTC timeline at the moment"

    if 'all' == args.timeline:
        args.timeline = None
    elif 'NRT' == args.timeline:
        args.timeline = "Near Real Time"
    elif 'NTC' == args.timeline:
        args.timeline = "Non Time Critical"

    if args.lat is not None and args.lon is not None:
        args.geom = utils.geojson_geometry_object(args.lat, args.lon,
                                                  args.width, args.height)

    get_time_series(args.geom,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    bands=args.band,
                    title=args.title,
                    orbit_direction=args.orbit_direction,
                    tml=args.timeline,
                    out_dir=args.outdir,
                    api=args.api,
                    mirror=args.mirror,
                    no_crop=args.no_crop,
                    product_type=args.product_type,
                    cloud_masks=args.cloud_masks,
                    parallel_downloads=args.parallel_downloads,
                    timeout=args.timeout)
