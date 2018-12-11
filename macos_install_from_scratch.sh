#!/bin/bash

# TSD dependencies installation from scratch for macOS.
# Copyright (C) 2017, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>

# This script was succesfully tested on a bare Mac mini Server running
# macOS 10.12 (rented at www.macstadium.com)

# It provides both python 2 and 3 with working bindings for gdal 2.1 with JP2 enabled.

# brew
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

# python2
brew install python
pip2 install --upgrade pip setuptools

# python3
brew install python3
pip3 install --upgrade pip setuptools wheel

# gdal
brew install wget
wget http://www.kyngchaos.com/files/software/frameworks/GDAL_Complete-2.1.dmg
X=$(echo `hdiutil mount GDAL_Complete-2.1.dmg | tail -1 | awk '{$1=$2=""; print $0}'` | xargs -0 echo) && sudo installer -pkg "${X}/"GDAL\ Complete.pkg -target /
echo 'PATH="/Library/Frameworks/GDAL.framework/Programs:$PATH"' >> ~/.profile
export PATH="/Library/Frameworks/GDAL.framework/Programs:$PATH"

# gdal python bindings and rasterio
for pip in pip2 pip3; do
    $pip install numpy
    $pip install rasterio gdal==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}') --global-option build_ext --global-option=`gdal-config --cflags` --global-option build_ext --global-option=-L`gdal-config  --prefix`/unix/lib/
done

brew install git fftw
git clone https://github.com/carlodef/tsd.git
pip2 install --user -r tsd/requirements.txt
pip3 install --user -r tsd/requirements.txt
python2 setup.py install
python3 setup.py install
