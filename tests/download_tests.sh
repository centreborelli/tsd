#!/bin/bash

python get_landsat.py --lon -102.5364 --lat 32.4396 -w 5000 -l 5000 -s 2015-04-05 -e 2015-07-20 -rd -o tests/l8
python get_sentinel2.py --lon -102.5364 --lat 32.4396 -w 5000 -l 5000 -s 2016-04-05 -e 2016-07-20 -rd -o tests/s2
