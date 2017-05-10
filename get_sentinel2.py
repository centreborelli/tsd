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
import os
import sys
import shutil
import argparse
import numpy as np
import utm
import dateutil.parser
import requests
import bs4
import matplotlib.path
import matplotlib.transforms

<<<<<<< HEAD
import search_sentinel2
import download_sentinel2
=======
>>>>>>> origin/master
import utils
import parallel
import search_scihub
import search_planet
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from stable.scripts.midway import midway_on_files
from stable.scripts import registration

<<<<<<< HEAD
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from stable.scripts.midway import midway_on_files
from stable.scripts import registration

from sortedcontainers import SortedSet
=======
>>>>>>> origin/master

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


def is_image_cloudy_at_location(image_aws_url, lat, lon, w=50):
    """
    Tell if the given location is covered by clouds in a given image (metadata).

    The location is considered covered if a cloud intersects the square of size
    w centered on the location.

    Args:
        image_aws_url: url of the image on AWS
        lat, lon: geographic coordinates of the input location
        w: width in meters of a square centred around (lat, lon)
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

    utm_x, utm_y = utm.from_latlon(lat, lon)[:2]
    for polygon in polygons:
        try:
            coords = list(map(int, polygon.posList.text.split()))
            points = list(zip(coords[::2], coords[1::2]))
            if polygon_intersects_bbox(points, [[utm_x - w, utm_y - w],
                                                [utm_x + w, utm_y + w]]):
                return True
        except IndexError:
            pass
    return False


def polygon_intersects_bbox(polygon, bbox):
    """
    Check if a polygon intersects a rectangle.

    Args:
        poly: list of 2D points defining a polygon. Each 2D point is represented
            by a pair of coordinates
        bbox: list of two opposite corners of the rectangle. Each corner is
            represented by a pair of coordinates

    Returns:
        True if the rectangle intersects the polygon
    """
    if np.array(polygon).ndim != 2:
        print('WARNING: wrong shape for polygon', polygon)
        return False
    else:
        p = matplotlib.path.Path(polygon)
        b = matplotlib.transforms.Bbox(bbox)
        return p.intersects_bbox(b)


def get_time_series(lat, lon, w, h, start_date=None, end_date=None, bands=[4],
                    out_dir='', search_api='planet', parallel_downloads=10,
                    register=False, equalize=False, debug=False):
    """
    Main function: download, crop and register a time series of Sentinel-2 images.
    """
    # list available images
    if search_api == 'scihub':
        images = search_scihub.search(lat, lon, w, h, start_date,
                                      end_date)['results']
    elif search_api == 'planet':
        images = search_planet.search(lat, lon, w, h, start_date, end_date,
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

    # compute coordinates of the crop
    cx, cy = utm.from_latlon(lat, lon)[:2]

    if register:  # take 100 meters margin in case of forthcoming shift
        w += 100
        h += 100

    ulx = cx - w / 2
    lrx = cx + w / 2
    uly = cy + h / 2  # in UTM the y coordinate increases from south to north
    lry = cy - h / 2

    # download crops
    utils.mkdir_p(out_dir)
    print('Downloading {} crops ({} images with {} bands)...'.format(len(urls),
                                                                     len(images),
                                                                     len(bands)))
    parallel.run_calls(utils.download_crop_with_gdal_vsicurl, zip(fnames,
                                                                  urls),
                       parallel_downloads, ulx, uly, lrx, lry)

    # discard images that are totally covered by clouds
    cloudy = []
    for img in images:
        url = aws_url_from_metadata_dict(img, search_api)
        name = filename_from_metadata_dict(img, search_api)
        if is_image_cloudy_at_location(url, lat, lon):
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
        if debug:  # keep a copy of the cropped images before registration
            bak = os.path.join(out_dir, 'no_registration')
            utils.mkdir_p(bak)
            for bands_fnames in crops:
                for f in bands_fnames:  # crop to remove the margin
                    o = os.path.join(bak, os.path.basename(f))
                    utils.crop_georeferenced_image(o, f, lon, lat, w-100, h-100)

        print('Registering...')
        registration.main(crops, crops, all_pairwise=True)

        for bands_fnames in crops:
            for f in bands_fnames:  # crop to remove the margin
                utils.crop_georeferenced_image(f, f, lon, lat, w-100, h-100)

    # equalize histograms through time, band per band
    if equalize:
        if debug:  # keep a copy of the images before equalization
<<<<<<< HEAD
            utils.mkdir_p(os.path.join(out_dir, 'no_midway'))
            for crop in crops:
                for b in crop:
                    shutil.copy(b, os.path.join(out_dir, 'no_midway'))

        for i in range(len(bands)):
            midway_on_files([crop[i] for crop in crops if len(crop) > i], out_dir)


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
        start_date = datetime.datetime(2000, 1, 1)
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
=======
            bak = os.path.join(out_dir, 'no_midway')
            utils.mkdir_p(bak)
            for bands_fnames in crops:
                for f in bands_fnames:
                    shutil.copy(f, bak)
>>>>>>> origin/master

        print('Equalizing...')
        for i in xrange(len(bands)):
            midway_on_files([crop[i] for crop in crops if len(crop) > i], out_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Automatic download and crop '
                                                  'of Sentinel-2 images'))
    parser.add_argument('--lat', type=utils.valid_lat, required=True,
                        help='latitude')
    parser.add_argument('--lon', type=utils.valid_lon, required=True,
                        help='longitude')
    parser.add_argument('-w', '--width', type=int, help='width of the crop, in meters',
                        default=5000)
    parser.add_argument('-l', '--height', type=int, help='height of the crop, in meters',
                        default=5000)
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
    parser.add_argument('--parallel-downloads', type=int, default=10,
                        help='max number of parallel crops downloads')
    args = parser.parse_args()

    get_time_series(args.lat, args.lon, args.width, args.height,
                    start_date=args.start_date, end_date=args.end_date,
                    bands=args.band, register=args.register,
                    equalize=args.midway, out_dir=args.outdir,
                    debug=args.debug, search_api=args.api,
                    parallel_downloads=args.parallel_downloads)
