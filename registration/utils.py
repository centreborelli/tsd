# vim: set fileencoding=utf-8
# pylint: disable=C0103

from __future__ import print_function
import os
import errno
import numpy as np
from osgeo import gdal
gdal.UseExceptions()


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
