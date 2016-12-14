# vim: set fileencoding=utf-8
# pylint: disable=C0103

from __future__ import print_function
import os
import errno
import argparse
import datetime
import tifffile
from osgeo import gdal
import numpy as np


def valid_date(s):
    """
    Check if a string is a well-formatted date.
    """
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid date: '{}'".format(s))


def is_valid(f):
    """
    Check if a path is a valid image file according to gdal.
    """
    try:
        a = gdal.Open(f); a = None  # gdal way of closing files
        return True
    except RuntimeError:
        return False


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


def get_geographic_info(filename):
    """
    Read the geographic information of an image file.
    """
    dataset = gdal.Open(filename)
    if dataset is None:
        print('Unable to open {} for reading'.format(filename))
        return
    return dataset.GetGeoTransform(), dataset.GetProjection()


def set_geographic_info(filename, geotransform, projection):
    """
    Write a given geographic information to an image file.

    Args:
        filename: path to the file where the information has to be written
        geotransform, projection: gdal geographic information
    """
    dataset = gdal.Open(filename, gdal.GA_Update)
    if dataset is None:
        print('Unable to open {} for writing'.format(filename))
        return

    if geotransform is not None and geotransform != (0, 1, 0, 0, 0, 1):
        dataset.SetGeoTransform(geotransform)

    if projection is not None and projection != '':
        dataset.SetProjection(projection)


def merge_bands(infiles, outfile):
    """
    Produce a multi-band tiff file from a sequence of mono-band tiff files.

    Args:
        infiles: list of paths to the input mono-bands images
        outfile: path to the ouput multi-band image file
    """
    tifffile.imsave(outfile, np.dstack(tifffile.imread(f) for f in infiles))
