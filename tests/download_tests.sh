#!/bin/bash

gdal_translate --config GDAL_DISABLE_READDIR_ON_OPEN YES --config CPL_VSIL_CURL_ALLOWED_EXTENSIONS TIF /vsicurl/http://landsat-pds.s3.amazonaws.com/L8/030/038/LC80300382015126LGN00/LC80300382015126LGN00_B8.TIF tests/l8.tif -of GTiff -srcwin 2400 3500 500 500
gdal_translate --config GDAL_DISABLE_READDIR_ON_OPEN YES --config CPL_VSIL_CURL_ALLOWED_EXTENSIONS jp2 /vsicurl/http://sentinel-s2-l1c.s3.amazonaws.com/tiles/13/S/GR/2016/5/11/0/B04.jp2 tests/s2.tif -of GTiff -ot UInt16 -srcwin 2400 3500 500 500
python get_landsat.py --lon -102.5364 --lat 32.4396 -w 5000 -l 5000 -s 2015-04-05 -e 2015-07-20 -o tests/l8
python get_sentinel2.py --lon -102.5364 --lat 32.4396 -w 5000 -l 5000 -s 2016-04-05 -e 2016-07-20 -o tests/s2
