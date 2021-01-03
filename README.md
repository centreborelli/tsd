# Time Series Downloader (TSD)
[![Build Status](https://travis-ci.com/cmla/tsd.svg?branch=master)](https://travis-ci.com/cmla/tsd)

Automatic download of Sentinel, Landsat and Planet crops.

[Carlo de Franchis](mailto:carlo.de-franchis@ens-cachan.fr),
CMLA, ENS Cachan, Université Paris-Saclay, 2016-19

With contributions from [Enric Meinhardt-Llopis](mailto:enric.meinhardt@cmla.ens-cachan.fr), [Axel Davy](mailto:axel.davy@ens.fr) and [Tristan Dagobert](mailto:tristan.dagobert@cmla.ens-cachan.fr).


The main source code repository for this software is https://github.com/cmla/tsd.

# Installation

`tsd` is easily installed from sources with `pip`:

    git clone https://github.com/cmla/tsd
    cd tsd
    pip install -e .

Alternatively, `tsd` latest release can also be installed from PyPI:

    pip install tsd


# Usage

Search and download is performed by `get_sentinel2.py`, `get_landsat.py`,
`get_planet.py` and `get_sentinel1.py` (one file per satellite constellation).
They can be used both as command line scripts or as Python modules.

They use the Python modules `search_stac.py`, `search_scihub.py`,
`search_peps.py` and `search_planet.py` (one file per API provider).

## From the command line
TSD can be used from the command line through the Python scripts
`get_*.py`. For instance, to download and process Sentinel-2 images of the
Jamnagar refinery, located at latitude 22.34806 and longitude 69.86889, run

    python get_sentinel2.py --lat 22.34806 --lon 69.86889 -b B02 B03 B04 -o test

This downloads crops of size 5000 x 5000 meters from the bands 2, 3 and 4,
corresponding to the blue, green and red channels, and stores them in geotif
files in the `test` directory.

It should print something like this on `stdout` (the number of images might vary):

    Found 22 images
    Elapsed time: 0:00:02.301129

    Downloading 66 crops (22 images with 3 bands)... 66 / 66
    Elapsed time: 0:00:57.620805

    Reading 22 cloud masks... 22 / 22
    6 cloudy images out of 22
    Elapsed time: 0:00:15.066992

Images with more than half of the pixels covered by clouds (according to the
cloud polygons available in Sentinel-2 images metadata, or Landsat-8 images
quality bands) are moved in the `test/cloudy` subfolder.

To specify the desired bands, use the `-b` or `--band` flag. The crop size can
be changed with the `--width` and `--height` flags. For instance

    python get_sentinel2.py --lat 22.34806 --lon 69.86889 -b B11 B12 --width 8000 --height 6000

downloads crops of size 8000 x 6000 meters, only for the SWIR channels (bands 11
and 12).

All the available options are listed with the `-h` or `--help` flag:

    python get_sentinel2.py -h

You can also run any of the `search_*.py` scripts from the command line
separately. Run them with `-h` to get the list of available options.  For a
nice output formatting, pipe their output to `jq` (`brew install jq`).

    python search_stac.py --lat 22.34806 --lon 69.86889 | jq

For example, this should print ready to use `curl` commands for downloading
Sentinel-5P netCDF files:

    python search_scihub.py --lon 2 --lat 48 -s 2020-3-1 --satellite Sentinel-5P --product-type L1B_RA_BD8 | jq -r '.[] | "curl --user s5pguest:s5pguest \"\(.links.alternative)\\$value\" > \(.title).nc"'


## As Python modules

The Python modules can be imported to call their functions from Python. Refer
to their docstrings to get usage information. Here are some examples.

    # define an area of interest
    import tsd
    lat, lon = 42, 3
    aoi = tsd.utils.geojson_geometry_object(lat, lon, 5000, 5000)

    # search Landsat-8 images available on the AOI with a STAC API
    x = tsd.search_stac.search(aoi, satellite='Landsat-8')


# Common issues

_Warning_: A `rasterio` issue on Ubuntu causes the need for this environment
variable (more info on [rasterio's
github](https://github.com/mapbox/rasterio/issues/942)):

    export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
