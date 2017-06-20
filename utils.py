# vim: set fileencoding=utf-8
# pylint: disable=C0103

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
import matplotlib.pyplot as plt
import traceback
import warnings
import sys
import geojson
import requests
import shapely.geometry
gdal.UseExceptions()


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
        except OSError as exc: # requires Python > 2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else: raise


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


def get_geotif_metadata(filename):
    """
    Read some metadata (using GDAL) from the header of a geotif file.
    """
    f = gdal.Open(filename)
    if f is None:
        print('Unable to open {} for reading'.format(filename))
        return
    return f.GetGeoTransform(), f.GetProjection(), f.GetMetadata()


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


def crop_with_gdal_translate(outpath, inpath, ulx, uly, lrx, lry, utm_zone=None):
    """
    """
    if outpath == inpath:  # hack to allow the output to overwrite the input
        fd, out = tempfile.mkstemp(suffix='.tif', dir=os.path.dirname(inpath))
        os.close(fd)
    else:
        out = outpath

    env = os.environ.copy()
    env['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = inpath[-3:]
    cmd = ['gdal_translate', inpath, out, '-ot', 'UInt16', '-of', 'GTiff',
           '-projwin', str(ulx), str(uly), str(lrx), str(lry)]
    if utm_zone is not None and gdal_translate_version() >= '2.0':
        cmd += ['-projwin_srs', '+proj=utm +zone={}'.format(utm_zone)]
    try:
        #print(' '.join(cmd))
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, env=env)
    except subprocess.CalledProcessError as e:
        if inpath.startswith('/vsicurl/'):
            url = inpath.split('vsicurl/')[1]
            if not requests.head(url).ok:
                print('{} is not available'.format(url))
                return
        print('ERROR: this command failed')
        print(' '.join(cmd))
        print(e.output)
        return

    if utm_zone is not None:
        # if the image is not sampled on the desired UTM grid, remove it
        img = gdal.Open(out)
        s = img.GetProjection()  # read geographic metadata
        img = None  # gdal way of closing files
        x = s.lower().split('utm zone ')[1][:2]  # hack to extract the UTM zone number
        if int(x) != utm_zone:
            os.remove(out)
            return

        # a better alternative would be to resample the image on the desired
        # UTM grid, but I don't know how to ensure that the resampled image will have
        # the exact same geographic bounding box as the others.
        #inplace_utm_reprojection_with_gdalwarp(out, utm_zone, ulx, uly, lrx, lry)

    if outpath == inpath:  # hack to allow the output to overwrite the input
        shutil.move(out, outpath)


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
    zone_number = utm.from_latlon(lat, lon)[2]

    # convert all polygon vertices coordinates from (lon, lat) to utm
    c = []
    for lon, lat in aoi['coordinates'][0]:
        c.append(utm.from_latlon(lat, lon, force_zone_number=zone_number)[:2])

    return geojson.Polygon([c])


def utm_bbx(aoi):
    """
    """
    # compute the utm zone number of the first polygon vertex
    lon, lat = aoi['coordinates'][0][0]
    zone_number = utm.from_latlon(lat, lon)[2]
    #zone_number = utm.latlon_to_zone_number(lat, lon)  # fails on older versions

    # convert all polygon vertices coordinates from (lon, lat) to utm
    c = []
    for lon, lat in aoi['coordinates'][0]:
        c.append(utm.from_latlon(lat, lon, force_zone_number=zone_number)[:2])

    # return the utm bounding box
    bbx = shapely.geometry.Polygon(c).bounds  # minx, miny, maxx, maxy
    return bbx[0], bbx[3], bbx[2], bbx[1], zone_number  # minx, maxy, maxx, miny


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


def weighted_median(data, weights=None):
    """
    Return the weighted median of a 1D array.
    """
    if weights is None:
        return np.median(data)

    sorted_data, sorted_weights = zip(*sorted(zip(data, weights)))
    cumulative_weight = np.cumsum(sorted_weights)
    total_weight = cumulative_weight[-1]

    if any(weights > total_weight * 0.5):
        return np.array(data)[weights == np.max(weights)][0]

    i = np.where(cumulative_weight <= .5 * total_weight)[0][-1] + 1
    left_right_diff = cumulative_weight[i-1] - (total_weight - cumulative_weight[i])
    if left_right_diff == 0:
        return data[i]
    elif left_right_diff > 0:
        return 0.5 * (sorted_data[i-1] + sorted_data[i])
    else:
        return 0.5 * (sorted_data[i] + sorted_data[i+1])


def show(img):
    """
    """
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
