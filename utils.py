# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Wrappers for gdal and rasterio.

Copyright (C) 2018, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>
"""

from __future__ import print_function
import os
import re
import errno
import shutil
import argparse
import datetime
import subprocess
import tempfile
import tifffile
from osgeo import gdal, osr
import numpy as np
import utm
import traceback
import warnings
import sys
import geojson
import requests
import shapely.geometry
import rasterio
gdal.UseExceptions()

import rpc_model


warnings.filterwarnings("ignore",
                        category=rasterio.errors.NotGeoreferencedWarning)


def download(from_url, to_file, auth=('', '')):
    """
    Download a file from an url to a file.
    """
    mkdir_p(os.path.dirname(to_file))
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
    Check if a path is a valid image file according to gdal.
    """
    try:
        a = gdal.Open(f); a = None  # gdal way of closing files
        return True
    except RuntimeError:
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


def mkdir_p(path):
    """
    Create a directory without complaining if it already exists.
    """
    if path:
        try:
            os.makedirs(path)
        except OSError as exc:  # requires Python > 2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise


def pixel_size(filename):
    """
    Read the resolution (in meters per pixel) of a geotif image.
    """
    f = gdal.Open(filename)
    if f is None:
        print('WARNING: Unable to open {} for reading'.format(filename))
        return
    try:
        # GetGeoTransform give a 6-uple containing tx, rx, 0, ty, 0, ry
        resolution = np.array(f.GetGeoTransform())[[1, 5]]
    except AttributeError:
        print('WARNING: Unable to retrieve {} GeoTransform'.format(filename))
        return
    f = None  # gdal way of closing files
    return resolution[0], -resolution[1]  # for gdal, ry < 0


def set_geotif_metadata(filename, geotransform=None, projection=None,
                        metadata=None):
    """
    Write some metadata (using GDAL) to the header of a geotif file.

    Args:
        filename: path to the file where the information has to be written
        geotransform, projection: gdal geographic information
        metadata: dictionary written to the GDAL 'Metadata' tag. It can be used
            to store any extra metadata (e.g. acquisition date, sun azimuth...)
    """
    f = gdal.Open(filename, gdal.GA_Update)
    if f is None:
        print('Unable to open {} for writing'.format(filename))
        return

    if geotransform is not None and geotransform != (0, 1, 0, 0, 0, 1):
        f.SetGeoTransform(geotransform)

    if projection is not None and projection != '':
        f.SetProjection(projection)

    if metadata is not None:
        f.SetMetadata(metadata)


def set_geotif_metadata_item(filename, tagname, tagvalue):
    """
    Append a key, value pair to the GDAL metadata tag to a geotif file.
    """
    dataset = gdal.Open(filename, gdal.GA_Update)
    if dataset is None:
        print('Unable to open {} for writing'.format(filename))
        return

    dataset.SetMetadataItem(tagname, tagvalue)


def merge_bands(infiles, outfile):
    """
    Produce a multi-band tiff file from a sequence of mono-band tiff files.

    Args:
        infiles: list of paths to the input mono-bands images
        outfile: path to the ouput multi-band image file
    """
    tifffile.imsave(outfile, np.dstack(tifffile.imread(f) for f in infiles))


def inplace_utm_reprojection_with_gdalwarp(src, utm_zone, ulx, uly, lrx, lry):
    """
    """
    img = gdal.Open(src)
    s = img.GetProjection()  # read geographic metadata
    img = None  # gdal way of closing files
    x = s.lower().split('utm zone ')[1][:2]  # hack to extract the UTM zone number
    if int(x) != utm_zone:

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


def crop_georeferenced_image(out_path, in_path, lon, lat, w, h):
    """
    Crop an image around a given geographic location.

    Args:
        out_path: path to the output (cropped) image file
        in_path: path to the input image file
        lon, lat: longitude and latitude of the center of the crop
        w, h: width and height of the crop, in meters
    """
    # compute utm geographic coordinates of the crop
    cx, cy = utm.from_latlon(lat, lon)[:2]
    ulx = cx - w / 2
    lrx = cx + w / 2
    uly = cy + h / 2  # in UTM the y coordinate increases from south to north
    lry = cy - h / 2

    if out_path == in_path:  # hack to allow the output to overwrite the input
        fd, tmp = tempfile.mkstemp(suffix='.tif', dir=os.path.dirname(in_path))
        os.close(fd)
        subprocess.check_output(['gdal_translate', in_path, tmp, '-ot',
                                 'UInt16', '-projwin', str(ulx), str(uly),
                                 str(lrx), str(lry)])
        shutil.move(tmp, out_path)
    else:
        subprocess.check_output(['gdal_translate', in_path, out_path, '-ot',
                                 'UInt16', '-projwin', str(ulx), str(uly),
                                 str(lrx), str(lry)])


def gdal_translate_version():
    """
    """
    v = subprocess.check_output(['gdal_translate', '--version'])
    return v.decode().split()[1].split(',')[0]


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
    if inpath.startswith(('http://', 'https://')):
        env['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = inpath[-3:]
        env['GDAL_DISABLE_READDIR_ON_OPEN'] = 'TRUE'
        env['VSI_CACHE'] = 'TRUE'
        path = '/vsicurl/{}'.format(inpath)
    elif inpath.startswith('s3://'):
        env['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = inpath[-3:]
        env['GDAL_DISABLE_READDIR_ON_OPEN'] = 'TRUE'
        env['VSI_CACHE'] = 'TRUE'
        env['AWS_REQUEST_PAYER'] = 'requester'
        path = '/vsis3/{}'.format(inpath[len('s3://'):])
    else:
        path = inpath

    cmd = ['gdal_translate', path, out, '-of', 'GTiff', '-projwin', str(ulx),
           str(uly), str(lrx), str(lry)]
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
    try:
        #print(' '.join(cmd))
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


def crop_with_gdalwarp(outpath, inpath, geojson_path):
    """
    """
    cmd = ['gdalwarp', inpath, outpath, '-ot', 'UInt16', '-of', 'GTiff',
           '-overwrite', '-crop_to_cutline', '-cutline', geojson_path]
    subprocess.check_output(cmd, stderr=subprocess.STDOUT)


def get_image_utm_zone(img_path):
    """
    Read the UTM zone from a geotif metadata.
    """
    img = gdal.Open(img_path)
    s = img.GetProjection()  # read geographic metadata
    img = None  # gdal way of closing files
    return s.lower().split('utm zone ')[1][:2]


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
        c.append(utm.from_latlon(lat, lon, force_zone_number=utm_zone)[:2])

    # utm bounding box
    bbx = shapely.geometry.Polygon(c).bounds  # minx, miny, maxx, maxy
    ulx, uly, lrx, lry = bbx[0], bbx[3], bbx[2], bbx[1]  # minx, maxy, maxx, miny

    if r is not None:  # round to multiples of the given resolution
        ulx = r * np.floor(ulx / r)
        uly = r * np.ceil(uly / r)
        lrx = r * np.ceil(lrx / r)
        lry = r * np.floor(lry / r)

    return ulx, uly, lrx, lry, utm_zone, lat_band


def latlon_to_pix(img, lat, lon):
   """
   Get the pixel coordinates of a geographic location in a georeferenced image.

   Args:
       img: path to the input image
       lat, lon: geographic coordinates of the input location

   Returns:
       x, y: pixel coordinates
   """
   # load the image dataset
   ds = gdal.Open(img)

   # get a geo-transform of the dataset
   try:
       gt = ds.GetGeoTransform()
   except AttributeError:
       return 0, 0

   # create a spatial reference object for the dataset
   srs = osr.SpatialReference()
   srs.ImportFromWkt(ds.GetProjection())

   # set up the coordinate transformation object
   ct = osr.CoordinateTransformation(srs.CloneGeogCS(), srs)

   # change the point locations into the GeoTransform space
   point1, point0 = ct.TransformPoint(lon, lat)[:2]

   # translate the x and y coordinates into pixel values
   x = (point1 - gt[0]) / gt[1]
   y = (point0 - gt[3]) / gt[5]
   return int(x), int(y)


def latlon_rectangle_centered_at(lat, lon, w, h):
    """
    """
    x, y, number, letter = utm.from_latlon(lat, lon)
    rectangle = []
    rectangle.append(utm.to_latlon(x - .5*w, y - .5*h, number, letter))
    rectangle.append(utm.to_latlon(x - .5*w, y + .5*h, number, letter))
    rectangle.append(utm.to_latlon(x + .5*w, y + .5*h, number, letter))
    rectangle.append(utm.to_latlon(x + .5*w, y - .5*h, number, letter))
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


#def show(img):
#    """
#    """
#    fig, ax = plt.subplots()
#    ax.imshow(img, interpolation='nearest')
#
#    def format_coord(x, y):
#        col = int(x + 0.5)
#        row = int(y + 0.5)
#        if col >= 0 and col < img.shape[1] and row >= 0 and row < img.shape[0]:
#            z = img[row, col]
#            return 'x={}, y={}, z={}'.format(col, row, z)
#        else:
#            return 'x={}, y={}'.format(col, row)
#
#    ax.format_coord = format_coord
#    plt.show()


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

        crop = fill_value * np.ones((src.count, h, w))
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
