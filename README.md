# Time Series Downloader (TSD)

[Carlo de Franchis](mailto:carlo.de-franchis@ens-cachan.fr),
CMLA, ENS Cachan, Université Paris-Saclay, 2016-17

Automatic download and registration of Sentinel, Landsat and Planet crops.

# Installation and dependencies
The main scripts are `get_landsat.py`, `get_sentinel1.py`, `get_sentinel2.py`
and `get_planet.py`.

They use the Python modules `search_devseed.py`, `search_scihub.py`,
`search_peps.py`, `search_planet.py` and `register.py`.

## Python packages
The required Python packages are listed in the file `requirements.txt`. They
can be installed with `pip`:

    pip install -r requirements.txt

## GDAL (on macOS)
There are at least two ways of installing `gdal`:

### Using brew

    brew install gdal --with-complete --with-python3 --HEAD

Note that this version doesn't support JP2 files (hence it will fail to get
Sentinel-2 crops from AWS).

### Using the [GDAL Complete Compatibility Framework](http://www.kyngchaos.com/files/software/frameworks/GDAL_Complete-2.1.dmg).

Download and install the `.dmg` file from the link above. Don't forget to
update your `PATH` and `PYTHONPATH` after the installation by copying these lines
in your `~/.profile`:

    export PATH="/Library/Frameworks/GDAL.framework/Programs:$PATH"
    export PYTHONPATH="/Library/Frameworks/GDAL.framework/Versions/2.1/Python/2.7/site-packages:$PYTHONPATH"

Note that this version supports JP2 files, but Python bindings are available only for Python 2.


## GDAL (Linux)
On Linux `gdal` and its Python bindings are usually straightforward to install
through your package manager.

    sudo apt-get update
    sudo apt-get install libgdal-dev gdal-bin python-gdal


# Usage

## From the command line
The pipeline can be used from the command line through the Python scripts
`get_*.py`. For instance, to download and process Sentinel-2 images of the
Jamnagar refinery, located at latitude 22.34806 and longitude 69.86889, run

    python get_sentinel2.py --lat 22.34806 --lon 69.86889 -b 2 3 4 -r -o test

This will download crops of size 5000 x 5000 meters from the bands 2, 3 and 4,
corresponding to the blue, green and red channels, and register them through
time. To specify the desired bands, use the `-b` or `--band` flag. The crop
size can be changed with the `--width` and `--height` flags. For instance

    python get_sentinel2.py --lat 22.34806 --lon 69.86889 -b 11 12 --width 8000 --height 6000

will download crops of size 8000 x 6000 meters, only for the SWIR channels (bands 11
and 12), without registration (no option `-r`).

All the available options are listed when using the `-h` or `--help` flag:

    python get_sentinel2.py -h

You can also run any of the `search_*.py` scripts or `registration.py` from
the command line separately to use only single blocks of the pipeline. Run them
with `-h` to get the list of available options.

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
