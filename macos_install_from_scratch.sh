#!/bin/bash

# TSD dependencies installation from scratch for macOS.
# Copyright (C) 2017-19, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>

# A previous version of this script was succesfully tested on a bare Mac mini Server running
# macOS 10.12 (rented at www.macstadium.com)

# It provides python 3, rasterio and gdal JP2 enabled.

# brew
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

# python3
brew install python3
pip3 install --upgrade pip setuptools wheel

# gdal
brew install wget
wget http://www.kyngchaos.com/files/software/frameworks/GDAL_Complete-2.3.dmg
X=$(echo `hdiutil mount GDAL_Complete-2.3.dmg | tail -1 | awk '{$1=$2=""; print $0}'` | xargs -0 echo) && sudo installer -pkg "${X}/"GDAL\ Complete.pkg -target /
echo 'PATH="/Library/Frameworks/GDAL.framework/Programs:$PATH"' >> ~/.profile
export PATH="/Library/Frameworks/GDAL.framework/Programs:$PATH"

# tsd
pip3 install numpy  # pre-requisite for rasterio
pip3 install --upgrade https://github.com/cmla/tsd/tarball/master --no-binary rasterio
