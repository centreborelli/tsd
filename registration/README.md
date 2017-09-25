# Image Time Series Registration (ITSR)

[![Build Status](https://travis-ci.com/carlodef/itsr.svg?token=q3ppoFukgX6NERpM7HRM&branch=master)](https://travis-ci.com/carlodef/itsr)

Automatic registration of satellite image time series.

[Carlo de Franchis](mailto:carlo.de-franchis@ens-cachan.fr),
CMLA, ENS Cachan, Université Paris-Saclay, 2016-17

With contributions from [Martin Rais](mailto:martus@gmail.com) and [Axel Davy](mailto:axel.davy@ens.fr).

# Installation and dependencies

## GDAL
Please refer to [TSD's
README.md](https://github.com/carlodef/tsd/blob/master/README.md) file for
instructions about how to install GDAL.


## Python packages
The required Python packages are listed in the file `requirements.txt`. They
can be installed with `pip`:

    pip install -r requirements.txt


# Usage

The main file is `registration.py`. It serves both as a Python module and as a script.


## From the command line
All the available options are listed when using the `-h` or `--help` flag:

    python registration.py -h


## As Python modules
The `registration` module can be imported to call its functions from Python. Refer
to their docstrings to get usage information.
