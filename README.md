# Time Series Downloader (TSD)
[![Build Status](https://travis-ci.com/cmla/tsd.svg?branch=master)](https://travis-ci.com/cmla/tsd)

Automatic download of Sentinel, Landsat and Planet crops.

[Carlo de Franchis](mailto:carlo.de-franchis@ens-cachan.fr),
CMLA, ENS Cachan, Université Paris-Saclay, 2016-18

With contributions from [Enric Meinhardt-Llopis](mailto:enric.meinhardt@cmla.ens-cachan.fr), [Axel Davy](mailto:axel.davy@ens.fr) and [Tristan Dagobert](mailto:tristan.dagobert@cmla.ens-cachan.fr).


# Usage

Search and download is performed by `get_sentinel2.py`, `get_landsat.py`,
`get_planet.py` and `get_sentinel1.py` (one file per satellite constellation).
They can be used both as command line scripts or as Python modules.

They use the Python modules `search_devseed.py`, `search_scihub.py`,
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

    python search_devseed.py --lat 22.34806 --lon 69.86889 | jq


## As Python modules

The Python modules can be imported to call their functions from Python. Refer
to their docstrings to get usage information. Here are some examples.

    # define an area of interest
    import utils
    lat, lon = 42, 3
    aoi = utils.geojson_geometry_object(lat, lon, 5000, 5000)

    # search Landsat-8 images available on the AOI with Development Seed's API
    import search_devseed
    x = search_devseed.search(aoi, satellite='Landsat-8')


# Installation and dependencies
_Note_: a shell script installing all the needed stuff (`brew`, `python`,
`gdal`...) on an empty macOS system is given in the file
[macos_install_from_scratch.sh](macos_install_from_scratch.sh).

## GDAL
The main dependencies are GDAL and rasterio. All the others can be installed
with `pip` as shown in the [next section](#python-packages).

### On macOS
There are several ways of installing `gdal`. I recommend option 1 as it
gives a version of gdal 2.2 that works with JP2 files.

#### Option 1: using the GDAL Complete Compatibility Framework.

[Download](http://www.kyngchaos.com/files/software/frameworks/GDAL_Complete-2.2.dmg)
and install the `.dmg` file. Update your `PATH` after the installation by
running this command:

    export PATH="/Library/Frameworks/GDAL.framework/Programs:$PATH"

Copy it in your `~/.profile`.

Then install the `rasterio` package with pip:

    pip install rasterio --global-option build_ext --global-option=`gdal-config --cflags` --global-option build_ext --global-option=-L`gdal-config  --prefix`/unix/lib/

_Note_: installation of `rasterio` with Python 3 requires `numpy`.

The four `--global-option build_ext` options tell `pip` where to find gdal
headers and libraries.

#### Option 2: using brew

    brew install gdal --with-complete

Note that this version doesn't support JP2 files (hence it will fail to get
Sentinel-2 crops). Moreover, the version currently bottled in brew is only 1.11
(as of 08/2017).

### On Linux
On Linux `gdal` is usually straightforward to install through your package
manager.

    sudo apt-get update
    sudo apt-get install libgdal-dev gdal-bin

## Python packages
The required Python packages are listed in the file `requirements.txt`. They
can be installed with `pip`:

    pip install -r requirements.txt

## Install TSD as a global python package
Once `gdal` is correctly installed on your machine you can run

    python setup.py install

`tsd` can also be installed without using `git`:

    pip install --upgrade https://github.com/cmla/tsd/tarball/master
