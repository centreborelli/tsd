# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Wrappers for gdal and rasterio.

Copyright (C) 2018, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>
"""

from __future__ import print_function
import os
import re
import shutil
import argparse
import datetime
import subprocess
import tempfile
import numpy as np
import utm
import traceback
import warnings
import sys
import geojson
import requests
import shapely.geometry
import rasterio
import pyproj

from tsd import rpc_model


warnings.filterwarnings("ignore",
                        category=rasterio.errors.NotGeoreferencedWarning)


def download(from_url, to_file, auth=('', '')):
    """
    Download a file from an url to a file.
    """
    to_file = os.path.abspath(os.path.expanduser(to_file))
    os.makedirs(os.path.dirname(to_file), exist_ok=True)
    response = requests.get(from_url, stream=True, auth=auth)
    with open(to_file, 'wb') as handle:
        for data in response.iter_content():
            handle.write(data)


def valid_datetime(s):
    """
    Check if a string is a well-formatted datetime.
    """
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid date: '{}'".format(s))


def valid_date(s):
    """
    Check if a string is a well-formatted date.
    """
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid date: '{}'".format(s))


def valid_lon(s):
    """
    Check if a string is a well-formatted longitude.

    Args:
        s (str): longitude expressed as a decimal floating number (e.g.
            5.7431861) or as degrees, minutes and seconds (e.g. 5d44'35.47"E)
    """
    try:
        return float(s)
    except ValueError:
        regex = r"(\d+)d(\d+)'([\d.]+)\"([WE])"
        m = re.match(regex, s)
        if m is None:
            raise argparse.ArgumentTypeError("Invalid longitude: '{}'".format(s))
        else:
            x = m.groups()
            lon = int(x[0]) + float(x[1]) / 60 + float(x[2]) / 3600
            if x[3] == 'W':
                lon *= -1
            return lon


def valid_lat(s):
    """
    Check if a string is a well-formatted latitude.

    Args:
        s (str): longitude expressed as a decimal floating number (e.g.
            -49.359053) or as degrees, minutes and seconds (e.g. 49d21'32.59"S)
    """
    try:
        return float(s)
    except ValueError:
        regex = r"(\d+)d(\d+)'([\d.]+)\"([NS])"
        m = re.match(regex, s)
        if m is None:
            raise argparse.ArgumentTypeError("Invalid latitude: '{}'".format(s))
        else:
            x = m.groups()
            lat = int(x[0]) + float(x[1]) / 60 + float(x[2]) / 3600
            if x[3] == 'S':
                lat *= -1
            return lat


def valid_geojson(filepath):
    """
    Check if a file contains valid geojson.
    """
    with open(filepath, 'r') as f:
        geo = geojson.load(f)
    if type(geo) == geojson.geometry.Polygon:
        return geo
    if type(geo) == geojson.feature.FeatureCollection:
        p = geo['features'][0]['geometry']
        if type(p) == geojson.geometry.Polygon:
            return p
    raise argparse.ArgumentTypeError('Invalid geojson: only polygons are supported')


def geojson_geometry_object(lat, lon, w, h):
    """
    """
    return geojson.Polygon([lonlat_rectangle_centered_at(lon, lat, w, h)])


def is_valid(f):
    """
    Check if a file is valid readable image according to rasterio.

    Args:
        f (str): path to a file

    Return:
        boolean telling wether or not the file is a valid image
    """
    try:
        a = rasterio.open(f, 'r')
        a.close()
        return True
    except rasterio.RasterioIOError:
        return False


def tmpfile(ext=''):
    """
    Creates a temporary file.

    Args:
        ext: desired file extension. The dot has to be included.

    Returns:
        absolute path to the created file
    """
    fd, out = tempfile.mkstemp(suffix=ext)
    os.close(fd)           # http://www.logilab.org/blogentry/17873
    return out


def pixel_size(path):
    """
    Read the resolution (in meters per pixel) of a GeoTIFF image.

    Args:
        path (string): path to a GeoTIFF image file

    Return:
        rx, ry (tuple): two floats giving the horizontal and vertical pixel size
    """
    with rasterio.open(path, 'r') as f:
        return f.res


def set_geotif_metadata_items(path, tags={}):
    """
    Append key, value pairs to the GDAL "metadata" tag of a GeoTIFF file.

    Args:
        path (str): path to a GeoTIFF file
        tags (dict): key, value pairs to be added to the "metadata" tag
    """
    with rasterio.open(path, 'r+') as dst:
        dst.update_tags(**tags)


def geotiff_utm_zone(path):
    """
    Read the UTM zone of a GeoTIFF image.

    Args:
        path (string): path to a GeoTIFF image file

    Return:
        int between 1 and 60 identifying the UTM zone
    """
    with rasterio.open(path, 'r') as f:
        return int(f.crs['init'][-2:])


def gdal_translate_version():
    """
    """
    v = subprocess.check_output(['gdal_translate', '--version'])
    return v.decode().split()[1].split(',')[0]


def inplace_utm_reprojection_with_gdalwarp(src, utm_zone, ulx, uly, lrx, lry):
    """
    """
    if geotiff_utm_zone(src) != utm_zone:

        # hack to allow the output to overwrite the input
        fd, dst = tempfile.mkstemp(suffix='.tif', dir=os.path.dirname(src))
        os.close(fd)

        cmd = ['gdalwarp', '-t_srs', '+proj=utm +zone={}'.format(utm_zone),
               '-te', str(ulx), str(lry), str(lrx), str(uly),  # xmin ymin xmax ymax
               '-overwrite', src, dst]
        print(' '.join(cmd))
        try:
            #print(' '.join(cmd))
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            shutil.move(dst, src)
        except subprocess.CalledProcessError as e:
            print('ERROR: this command failed')
            print(' '.join(cmd))
            print(e.output)


def crop_with_gdal_translate(outpath, inpath, ulx, uly, lrx, lry,
                             utm_zone=None, lat_band=None, output_type=None):
    """
    """
    if outpath == inpath:  # hack to allow the output to overwrite the input
        fd, out = tempfile.mkstemp(suffix='.tif', dir=os.path.dirname(inpath))
        os.close(fd)
    else:
        out = outpath

    env = os.environ.copy()

    # these GDAL configuration options speed up the access to remote files
    if inpath.startswith(('http://', 'https://', 's3://', 'gs://')):
        env['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = inpath[-3:]
        env['GDAL_DISABLE_READDIR_ON_OPEN'] = 'TRUE'
        env['VSI_CACHE'] = 'TRUE'
        env['GDAL_HTTP_MAX_RETRY'] = '10000'  # needed for storage.googleapis.com 503
        env['GDAL_HTTP_RETRY_DELAY'] = '1'

    # add the relevant "/vsi" prefix to the input url
    if inpath.startswith(('http://', 'https://')):
        path = '/vsicurl/{}'.format(inpath)
    elif inpath.startswith('s3://'):
        env['AWS_REQUEST_PAYER'] = 'requester'
        path = '/vsis3/{}'.format(inpath[len('s3://'):])
    elif inpath.startswith('gs://'):
        path = '/vsicurl/http://storage.googleapis.com/{}'.format(inpath[len('gs://'):])
    else:
        path = inpath

    # build the gdal_translate shell command
    cmd = ['gdal_translate', path, out, '-of', 'GTiff', '-co', 'COMPRESS=DEFLATE',
           '-projwin', str(ulx), str(uly), str(lrx), str(lry)]
    if output_type is not None:
        cmd += ['-ot', output_type]
    if utm_zone is not None:
        if gdal_translate_version() < '2.0':
            print('WARNING: utils.crop_with_gdal_translate argument utm_zone requires gdal >= 2.0')
        else:
            srs = '+proj=utm +zone={}'.format(utm_zone)
            # latitude bands in the southern hemisphere range from 'C' to 'M'
            if lat_band and lat_band < 'N':
                srs += ' +south'
            cmd += ['-projwin_srs', srs]
            #print(' '.join(cmd))

    # run the gdal_translate command
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
    except subprocess.CalledProcessError as e:
        if inpath.startswith(('http://', 'https://')):
            if not requests.head(inpath).ok:
                print('{} is not available'.format(inpath))
                return
        print('ERROR: this command failed')
        print(' '.join(cmd))
        print(e.output)
        return

    if outpath == inpath:  # hack to allow the output to overwrite the input
        shutil.move(out, outpath)


def get_crop_from_aoi(output_path, aoi, metadata_dict, band):
    """
    Crop and download an AOI from a georeferenced image.

    Args:
        output_path (string): path to the output GeoTIFF file
        aoi (geojson.Polygon): area of interest defined by a polygon in longitude, latitude coordinates
        metadata_dict (dict): metadata dictionary
        band (str): desired band, e.g. 'B04' for Sentinel-2 or 'B8' for Landsat-8
    """
    try:  # Sentinel-2
        inpath = metadata_dict['urls']['gcloud'][band]
    except KeyError:  # Landsat-8
        inpath = metadata_dict['assets'][band]['href']
    utm_zone = int(metadata_dict['utm_zone']) if 'utm_zone' in metadata_dict else None
    ulx, uly, lrx, lry, utm_zone, lat_band = utm_bbx(aoi, utm_zone=utm_zone,
                                                     r=60)
    crop_with_gdal_translate(output_path, inpath, ulx, uly, lrx, lry, utm_zone,
                             lat_band)


def crop_with_gdalwarp(outpath, inpath, geojson_path):
    """
    """
    cmd = ['gdalwarp', inpath, outpath, '-ot', 'UInt16', '-of', 'GTiff',
           '-overwrite', '-crop_to_cutline', '-cutline', geojson_path]
    subprocess.check_output(cmd, stderr=subprocess.STDOUT)


def geojson_lonlat_to_utm(aoi):
    """
    """
    # compute the utm zone number of the first polygon vertex
    lon, lat = aoi['coordinates'][0][0]
    utm_zone = utm.from_latlon(lat, lon)[2]

    # convert all polygon vertices coordinates from (lon, lat) to utm
    c = []
    for lon, lat in aoi['coordinates'][0]:
        c.append(utm.from_latlon(lat, lon, force_zone_number=utm_zone)[:2])

    return geojson.Polygon([c])


def utm_bbx(aoi, utm_zone=None, r=None):
    """
    """
    lon, lat = aoi['coordinates'][0][0]
    if utm_zone is None:  # compute the utm zone number of the first vertex
        utm_zone, lat_band = utm.from_latlon(lat, lon)[2:]
    else:
        lat_band = utm.from_latlon(lat, lon, force_zone_number=utm_zone)[3]

    # convert all polygon vertices coordinates from (lon, lat) to utm
    c = []
    for lon, lat in aoi['coordinates'][0]:
        hemisphere = 'north' if lat>=0 else 'south'
        x,y = pyproj.transform(pyproj.Proj('+proj=latlong'), pyproj.Proj('+proj=utm +zone={}{} +{}'.format(utm_zone,lat_band,hemisphere)), lon, lat)
        c.append((x,y))

    # utm bounding box
    bbx = shapely.geometry.Polygon(c).bounds  # minx, miny, maxx, maxy
    ulx, uly, lrx, lry = bbx[0], bbx[3], bbx[2], bbx[1]  # minx, maxy, maxx, miny

    if r is not None:  # round to multiples of the given resolution
        ulx = r * np.round(ulx / r)
        uly = r * np.round(uly / r)
        lrx = r * np.round(lrx / r)
        lry = r * np.round(lry / r)

    return ulx, uly, lrx, lry, utm_zone, lat_band



def latlon_to_pix(path, lat, lon):
   """
   Get the pixel coordinates of a geographic location in a GeoTIFF image.

   Args:
       path: path to the input image
       lat, lon: geographic coordinates of the input location

   Returns:
       x, y: pixel coordinates
   """
   with rasterio.open(path, 'r') as f:
       crs = f.crs
       transform = f.transform

   # transform (lon, lat) to the coordinate reference system of the image
   x, y = pyproj.transform(pyproj.Proj('+proj=latlong'), pyproj.Proj(crs),
                           lon, lat)

   # transform x, y to pixel coordinates
   return ~transform * (x, y)


def latlon_rectangle_centered_at(lat, lon, w, h):
    """
    """
    x, y, number, letter = utm.from_latlon(lat, lon)
    rectangle = [utm.to_latlon(x - .5*w, y - .5*h, number, letter),
                 utm.to_latlon(x - .5*w, y + .5*h, number, letter),
                 utm.to_latlon(x + .5*w, y + .5*h, number, letter),
                 utm.to_latlon(x + .5*w, y - .5*h, number, letter)]
    rectangle.append(rectangle[0])  # close the polygon
    return rectangle


def lonlat_rectangle_centered_at(lon, lat, w, h):
    """
    """
    return [p[::-1] for p in latlon_rectangle_centered_at(lat, lon, w, h)]


def print_elapsed_time(since_first_call=False):
    """
    Print the elapsed time since the last call or since the first call.

    Args:
        since_first_call:
    """
    t2 = datetime.datetime.now()
    if since_first_call:
        print("Total elapsed time:", t2 - print_elapsed_time.t0)
    else:
        try:
            print("Elapsed time:", t2 - print_elapsed_time.t1)
        except AttributeError:
            print("Elapsed time:", t2 - print_elapsed_time.t0)
    print_elapsed_time.t1 = t2
    print()


def show(img):
    """
    """
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.imshow(img, interpolation='nearest')

    def format_coord(x, y):
        col = int(x + 0.5)
        row = int(y + 0.5)
        if col >= 0 and col < img.shape[1] and row >= 0 and row < img.shape[0]:
            z = img[row, col]
            return 'x={}, y={}, z={}'.format(col, row, z)
        else:
            return 'x={}, y={}'.format(col, row)

    ax.format_coord = format_coord
    plt.show()


def warn_with_traceback(message, category, filename, lineno, file=None,
                        line=None):
    traceback.print_stack()
    log = file if hasattr(file,'write') else sys.stderr
    log.write(warnings.formatwarning(message, category, filename, lineno, line))

#warnings.showwarning = warn_with_traceback


def bounding_box2D(pts):
    """
    bounding box for the points pts
    """
    dim = len(pts[0])  # should be 2
    bb_min = [min([t[i] for t in pts]) for i in range(dim)]
    bb_max = [max([t[i] for t in pts]) for i in range(dim)]
    return bb_min[0], bb_min[1], bb_max[0] - bb_min[0], bb_max[1] - bb_min[1]


def rpc_from_geotiff(geotiff_path):
    """
    """
    with rasterio.open(geotiff_path, 'r') as src:
        rpc_dict = src.tags(ns='RPC')
    return rpc_model.RPCModel(rpc_dict)


def points_apply_homography(H, pts):
    """
    Applies an homography to a list of 2D points.

    Args:
        H: numpy array containing the 3x3 homography matrix
        pts: numpy array containing the list of 2D points, one per line

    Returns:
        a numpy array containing the list of transformed points, one per line
    """
    pts = np.asarray(pts)

    # convert the input points to homogeneous coordinates
    if len(pts[0]) < 2:
        print("""points_apply_homography: ERROR the input must be a numpy array
          of 2D points, one point per line""")
        return
    pts = np.hstack((pts[:, 0:2], pts[:, 0:1]*0+1))

    # apply the transformation
    Hpts = (np.dot(H, pts.T)).T

    # normalize the homogeneous result and trim the extra dimension
    Hpts = Hpts * (1.0 / np.tile( Hpts[:, 2], (3, 1)) ).T
    return Hpts[:, 0:2]


def bounding_box_of_projected_aoi(rpc, aoi, z=0, homography=None):
    """
    Return the x, y, w, h pixel bounding box of a projected AOI.

    Args:
        rpc (rpc_model.RPCModel): RPC camera model
        aoi (geojson.Polygon): GeoJSON polygon representing the AOI
        z (float): altitude of the AOI with respect to the WGS84 ellipsoid
        homography (2D array, optional): matrix of shape (3, 3) representing an
            homography to be applied to the projected points before computing
            their bounding box.

    Return:
        x, y (ints): pixel coordinates of the top-left corner of the bounding box
        w, h (ints): pixel dimensions of the bounding box
    """
    lons, lats = np.array(aoi['coordinates'][0]).T
    x, y = rpc.projection(lons, lats, z)
    pts = list(zip(x, y))
    if homography is not None:
        pts = points_apply_homography(homography, pts)
    return np.round(bounding_box2D(pts)).astype(int)


class CropOutside(Exception):
    """
    Exception to raise when attempting to crop outside of the input image.
    """
    pass


def rasterio_crop(filename, x, y, w, h, boundless=True, fill_value=0):
    """
    Read a crop from a file with rasterio and return it as an array.

    This is a working alternative to this rasterio oneliner which currently fails:
    src.read(window=((y, y + h), (x, x + w)), boundless=True, fill_value=0)

    Args:
        filename: path to the input image file
        x, y: pixel coordinates of the top-left corner of the crop
        w, h: width and height of the crop, in pixels
        boundless (bool): similar to gdal_translate "epo: error when partially
            outside" flag. If False, we'll raise an exception when the
            requested crop is not entirely contained within the input image
            bounds. If True, the crop is padded with fill_value.
        fill_value (scalar): constant value used to fill pixels outside of the
            input image.
    """
    with rasterio.open(filename, 'r') as src:
        if not boundless:
            if y < 0 or y + h > src.shape[0] or x < 0 or x + w > src.shape[1]:
                raise CropOutside(('crop {} {} {} {} falls outside of input image '
                                   'whose shape is {}'.format(x, y, w, h, src.shape)))

        crop = fill_value * np.ones((src.count, h, w), dtype=src.profile['dtype'])
        y0 = max(y, 0)
        y1 = min(y + h, src.shape[0])
        x0 = max(x, 0)
        x1 = min(x + w, src.shape[1])
        crop[:, y0 - y:y1 - y, x0 - x:x1 - x] = src.read(window=((y0, y1), (x0, x1)))

    # interleave channels
    return np.moveaxis(crop, 0, 2).squeeze()


def crop_aoi(geotiff, aoi, z=0):
    """
    Crop a geographic AOI in a georeferenced image using its RPC functions.

    Args:
        geotiff (string): path or url to the input GeoTIFF image file
        aoi (geojson.Polygon): GeoJSON polygon representing the AOI
        z (float, optional): base altitude with respect to WGS84 ellipsoid (0
            by default)

    Return:
        crop (array): numpy array containing the cropped image
        x, y, w, h (ints): image coordinates of the crop. x, y are the
            coordinates of the top-left corner, while w, h are the dimensions
            of the crop.
    """
    x, y, w, h = bounding_box_of_projected_aoi(rpc_from_geotiff(geotiff), aoi, z)
    return rasterio_crop(geotiff, x, y, w, h), x, y


def rio_dtype(numpy_dtype):
    """
    Convert a numpy datatype to a rasterio datatype.
    """
    if numpy_dtype == 'bool':
        return rasterio.dtypes.bool_
    elif numpy_dtype == 'uint8':
        return rasterio.dtypes.uint8
    elif numpy_dtype == 'uint16':
        return rasterio.dtypes.uint16
    elif numpy_dtype == 'int16':
        return rasterio.dtypes.int16
    elif numpy_dtype == 'uint32':
        return rasterio.dtypes.uint32
    elif numpy_dtype == 'int32':
        return rasterio.dtypes.int32
    elif numpy_dtype == 'float32':
        return rasterio.dtypes.float32
    elif numpy_dtype == 'float64':
        return rasterio.dtypes.float64
    elif numpy_dtype == 'complex':
        return rasterio.dtypes.complex_
    elif numpy_dtype == 'complex64':
        return rasterio.dtypes.complex64
    elif numpy_dtype == 'complex128':
        return rasterio.dtypes.complex128


def rio_write(path, array, profile={}, tags={}, namespace_tags={}):
    """
    Write a numpy array in a tiff/png/jpeg file with rasterio.

    Args:
        path: path to the output tiff/png/jpeg file
        array: 2D or 3D numpy array containing the image to write
        profile: rasterio profile (ie dictionary of metadata)
        tags: dictionary of additional geotiff tags
        namespace_tags: dictionary of dictionaries of additional geotiff tags
            (e.g.  IMAGE_STRUCTURE, RPC, SUBDATASETS)
    """
    # read image size and number of bands
    if array.ndim > 2:
        height, width, nbands = array.shape
    else:
        nbands = 1
        height, width = array.shape

    # determine the driver based on the file extension
    extension = os.path.splitext(path)[1].lower()
    if extension in ['.tif', '.tiff']:
        driver = 'GTiff'
    elif extension in ['.jpg', '.jpeg']:
        driver = 'jpeg'
    elif extension in ['.png']:
        driver = 'png'
    else:
        print('ERROR: unknown extension {}'.format(extension))

    with warnings.catch_warnings():  # noisy may occur here
        warnings.filterwarnings("ignore", category=FutureWarning)
        profile.update(driver=driver, count=nbands, width=width, height=height,
                       dtype=rio_dtype(array.dtype), quality=100)
        with rasterio.open(path, 'w', **profile) as dst:
            if array.ndim > 2:
                dst.write(np.moveaxis(array, 2, 0))
            else:
                dst.write(np.array([array]))
            dst.update_tags(**tags)
            for k, v in namespace_tags.items():
                dst.update_tags(ns=k, **v)
