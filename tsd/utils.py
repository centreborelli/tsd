# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Wrappers for gdal and rasterio.

Copyright (C) 2018, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>
"""

import os
import re
import argparse
import datetime
import warnings
import shutil

import numpy as np
import utm
import geojson
import requests
import rasterio
import rasterio.warp
import pyproj


warnings.filterwarnings("ignore",
                        category=rasterio.errors.NotGeoreferencedWarning)


def download(from_url, to_file, auth=None):
    """
    Download a file from an url to a file.
    """
    to_file = os.path.abspath(os.path.expanduser(to_file))
    os.makedirs(os.path.dirname(to_file), exist_ok=True)
    with requests.get(from_url, stream=True, auth=auth) as r:
        with open(to_file, 'wb') as handle:
            shutil.copyfileobj(r.raw, handle)


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
        s = s.replace(" ", "")
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
        s = s.replace(" ", "")
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
    if type(geo) == geojson.feature.Feature:
        p = geo['geometry']
        if type(p) == geojson.geometry.Polygon:
            return p
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
        with rasterio.open(f, "r"):
            pass
    except rasterio.RasterioIOError:
        return False

    return True


def set_geotif_metadata_items(path, tags={}):
    """
    Append key, value pairs to the GDAL "metadata" tag of a GeoTIFF file.

    Args:
        path (str): path to a GeoTIFF file
        tags (dict): key, value pairs to be added to the "metadata" tag
    """
    with rasterio.open(path, 'r+') as dst:
        dst.update_tags(**tags)


def rasterio_geo_crop(outpath, inpath, ulx, uly, lrx, lry, epsg=None,
                      output_type=None, debug=False):
    """
    Write a crop to disk from an input image, given the coordinates of the geographical
    bounding box.

    Args:
        outpath (str): path to the output crop
        inpath (str): path to the input image
        ulx, uly, lrx, lry (float): geographical coordinates of the crop bounding box
        epsg (int): EPSG code of the coordinate system in which the bounding box
            coordinates are expressed. If None, it is assumed that the coordinates
            are expressed in the CRS of the input image.
        output_type (str): output type of the crop
    """
    gdal_options = dict()

    # these GDAL configuration options speed up the access to remote files
    if inpath.startswith(("http://", "https://", "s3://")):
        _, file_ext = os.path.splitext(inpath)
        file_ext = file_ext[1:]  # Remove the leading dot from file_ext
        gdal_options["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = file_ext
        gdal_options["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
        gdal_options["VSI_CACHE"] = "TRUE"
        gdal_options["GDAL_HTTP_MAX_RETRY"] = "100"  # needed for storage.googleapis.com 503
        gdal_options["GDAL_HTTP_RETRY_DELAY"] = "1"

    if debug:
        left = ulx
        bottom = lry
        right = lrx
        top = uly
        print('AWS_REQUEST_PAYER=requester rio clip {} {} --bounds "{} {} {} {}" --geographic'.format(inpath, outpath, left, bottom, right, top))
        #print('AWS_REQUEST_PAYER=requester gdal_translate /vsis3/{} {} -projwin {} {} {} {}'.format(inpath[5:], outpath, ulx, uly, lrx, lry))

    if inpath.startswith("s3://"):
        session = rasterio.session.AWSSession(requester_pays=True)
    else:
        session = None

    bounds = ulx, lry, lrx, uly
    with rasterio.Env(session=session, **gdal_options):
        try:
            with rasterio.open(inpath) as src:

                # Convert the bounds to the CRS of inpath if epsg is given
                if epsg:
                    bounds = rasterio.warp.transform_bounds(epsg, src.crs, *bounds)

                # Get the pixel coordinates of the bounds in inpath
                window = src.window(*bounds)

                # Do a "floor" operation on offsets to match what gdal_translate does
                window = window.round_offsets()

                # Do a "round" operation on lengths to match what gdal_translate does
                width = round(window.width)
                height = round(window.height)
                window = rasterio.windows.Window(window.col_off, window.row_off, width, height)

                profile = src.profile
                transform = src.window_transform(window)
                crop = rasterio_window_crop(src, window.col_off, window.row_off, width, height)

        except rasterio.errors.RasterioIOError:
            print("WARNING: download of {} failed".format(inpath))
            return

        profile.update({"driver": "GTiff",
                        "compress": "deflate",
                        "height": height,
                        "width": width,
                        "transform": transform})
        if output_type:
            profile["dtype"] = output_type.lower()

        with rasterio.open(outpath, "w", **profile) as out:
            out.write(crop)


def crop_with_gdalwarp(outpath, inpath, ulx, uly, lrx, lry, epsg=None):
    """
    """
    if inpath.startswith(("http://", "https://")):
        inpath = "/vsicurl/{}".format(inpath)
    inpath = inpath.replace("s3://", "/vsis3/")

    if inpath.endswith("$value"):  # scihub urls special case
        file_ext = "value"
    else:
        _, file_ext = os.path.splitext(inpath)
        file_ext = file_ext[1:]  # Remove the leading dot from file_ext

    cmd = "GDAL_HTTP_USERPWD={}:{}".format(os.environ['COPERNICUS_LOGIN'],
                                           os.environ['COPERNICUS_PASSWORD'])
    cmd += " CPL_VSIL_CURL_ALLOWED_EXTENSIONS={}".format(file_ext)
    cmd += " GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR"
    cmd += " GDAL_INGESTED_BYTES_AT_OPEN=YES"
    cmd += " GDAL_HTTP_MERGE_CONSECUTIVE_RANGES=YES"
    cmd += " GDAL_HTTP_MULTIPLEX=YES"
    cmd += " GDAL_HTTP_VERSION=2"
    cmd += " CPL_VSIL_CURL_CHUNK_SIZE=2000000"  # 2MB
    cmd += " GDAL_HTTP_MAX_RETRY=3"
    cmd += " GDAL_HTTP_RETRY_DELAY=15"
    cmd += " VSI_CACHE=TRUE"
    cmd += " AWS_REQUEST_PAYER=requester"
    cmd += " gdalwarp \"{}\" {}".format(inpath, outpath)
    if epsg:
        cmd += " -t_srs epsg:{}".format(epsg)
    cmd += " -tr 10 10"
    cmd += " -te {} {} {} {}".format(ulx, lry, lrx, uly)
    cmd += " -q -overwrite"
    #print(cmd)
    os.system(cmd)


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
    epsg = int(metadata_dict['epsg']) if 'epsg' in metadata_dict else None
    ulx, uly, lrx, lry, epsg = utm_bbx(aoi, epsg=epsg, r=60)
    rasterio_geo_crop(output_path, inpath, ulx, uly, lrx, lry, epsg)


def utm_to_epsg_code(utm_zone, lat_band):
    """
    Computes an EPSG code number from a UTM zone and latitude band

    EPSG = CONST + UTM_ZONE where CONST is
    - 32600 for positive latitudes
    - 32700 for negative latitudes

    Args:
        utm_zone (int): integer between 1 and 60 indicating the UTM longitude zone
        lat_band (str): letter between C and X, excluding I and O, indicating the
            UTM latitude band

    Returns:
        int: integer indicating the EPSG code of the UTM zone
    """
    const = 32600 if lat_band >= "N" else 32700
    epsg = const + utm_zone
    return epsg


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


def compute_epsg(lon, lat):
    """
    Compute the EPSG code of the UTM zone which contains
    the point with given longitude and latitude

    Args:
        lon (float): longitude of the point
        lat (float): latitude of the point

    Returns:
        int: EPSG code
    """
    # UTM zone number starts from 1 at longitude -180,
    # and increments by 1 every 6 degrees of longitude
    zone = int((lon + 180) // 6 + 1)

    # EPSG = CONST + ZONE where CONST is
    # - 32600 for positive latitudes
    # - 32700 for negative latitudes
    const = 32600 if lat > 0 else 32700
    return const + zone


def pyproj_transform(x, y, in_crs, out_crs, z=None):
    """
    Wrapper around pyproj to convert coordinates from an EPSG system to another.

    Args:
        x (scalar or array): x coordinate(s), expressed in in_crs
        y (scalar or array): y coordinate(s), expressed in in_crs
        in_crs (pyproj.crs.CRS or int): input coordinate reference system or EPSG code
        out_crs (pyproj.crs.CRS or int): output coordinate reference system or EPSG code
        z (scalar or array): z coordinate(s), expressed in in_crs

    Returns:
        scalar or array: x coordinate(s), expressed in out_crs
        scalar or array: y coordinate(s), expressed in out_crs
        scalar or array (optional if z): z coordinate(s), expressed in out_crs
    """
    transformer = pyproj.Transformer.from_crs(in_crs, out_crs, always_xy=True)
    if z is None:
        return transformer.transform(x, y)
    else:
        return transformer.transform(x, y, z)


def utm_bbx(aoi, epsg=None, r=None, offset=(0, 0)):
    """
    Compute UTM bounding box of a longitude, latitude AOI.

    Args:
        aoi (geojson.Polygon): area of interest, defined as a (lon, lat) polygon
        epsg (int): EPSG code of the desired UTM zone
        r (int): if not None, round bounding box vertices to vertices of an
            r-periodic grid
        offset (tuple): origin of the r-periodic grid

    Returns:
        ulx, uly, lrx, lry (floats): bounding box upper left (ul) and lower
            right (lr) x, y coordinates
        epsg (int): EPSG code of the UTM zone
    """
    if epsg is None:  # compute the EPSG code of the AOI centroid
        lon, lat = np.mean(aoi['coordinates'][0][:-1], axis=0)
        epsg = compute_epsg(lon, lat)

    # convert all polygon vertices coordinates from (lon, lat) to utm
    lons, lats = np.asarray(aoi['coordinates'][0]).T
    xs, ys = pyproj_transform(lons, lats, 4326, epsg)
    c = list(zip(xs, ys))

    # utm bounding box
    x, y, w, h = bounding_box2D(c)  # minx, miny, width, height
    ulx, uly, lrx, lry = x, y + h, x + w, y

    if r is not None:  # round to multiples of the given resolution
        ox, oy = offset
        ulx = ox + r * np.round((ulx - ox) / r)
        uly = oy + r * np.round((uly - oy) / r)
        lrx = ox + r * np.round((lrx - ox) / r)
        lry = oy + r * np.round((lry - oy) / r)

    return ulx, uly, lrx, lry, epsg


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


def bounding_box2D(pts):
    """
    Rectangular bounding box for a list of 2D points.

    Args:
        pts (list): list of 2D points represented as 2-tuples or lists of length 2

    Returns:
        x, y, w, h (floats): coordinates of the top-left corner, width and
            height of the bounding box
    """
    dim = len(pts[0])  # should be 2
    bb_min = [min([t[i] for t in pts]) for i in range(dim)]
    bb_max = [max([t[i] for t in pts]) for i in range(dim)]
    return bb_min[0], bb_min[1], bb_max[0] - bb_min[0], bb_max[1] - bb_min[1]


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
        rpc (rpcm.RPCModel): RPC camera model
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


def rasterio_window_crop(src, x, y, w, h, boundless=True, fill_value=0):
    """
    Read a crop from a rasterio dataset and return it as an array.

    This uses rasterio's windowed reading functionality.

    Args:
        src: rasterio dataset opened in read mode
        x, y: pixel coordinates of the top-left corner of the crop
        w, h: width and height of the crop, in pixels
        boundless (bool): similar to gdal_translate "epo: error when partially
            outside" flag. If False, we'll raise an exception when the
            requested crop is not entirely contained within the input image
            bounds. If True, the crop is padded with fill_value.
        fill_value (scalar): constant value used to fill pixels outside of the
            input image.
    """
    if not boundless:
        if y < 0 or y + h > src.shape[0] or x < 0 or x + w > src.shape[1]:
            raise CropOutside(('crop {} {} {} {} falls outside of input image '
                               'whose shape is {}'.format(x, y, w, h, src.shape)))

    window = rasterio.windows.Window(x, y, w, h)
    return src.read(window=window, boundless=boundless, fill_value=fill_value)


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
    import rpcm
    x, y, w, h = bounding_box_of_projected_aoi(rpcm.rpc_from_geotiff(geotiff), aoi, z)
    with rasterio.open(geotiff) as src:
        crop = rasterio_window_crop(src, x, y, w, h)
    return crop, x, y


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
