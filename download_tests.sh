#!/bin/bash

mkdir -p tests
gdal_translate --config GDAL_DISABLE_READDIR_ON_OPEN YES --config CPL_VSIL_CURL_ALLOWED_EXTENSIONS TIF /vsicurl/http://landsat-pds.s3.amazonaws.com/c1/L8/176/039/LC08_L1TP_176039_20180614_20180703_01_T1/LC08_L1TP_176039_20180614_20180703_01_T1_B8.TIF tests/l8.tif -of GTiff -projwin 317533 3320455 322533 3315455 -projwin_srs "+proj=utm +zone=36"
gdal_translate --config GDAL_DISABLE_READDIR_ON_OPEN YES --config CPL_VSIL_CURL_ALLOWED_EXTENSIONS jp2 /vsicurl/http://storage.googleapis.com/gcp-public-data-sentinel-2/tiles/36/R/UU/S2A_MSIL1C_20180728T082601_N0206_R021_T36RUU_20180728T104559.SAFE/GRANULE/L1C_T36RUU_A016177_20180728T084109/IMG_DATA/T36RUU_20180728T082601_B04.jp2 tests/s2.tif -of GTiff -ot UInt16 -projwin 317520 3320460 322560 3315480 -projwin_srs "+proj=utm +zone=36"
python3 tsd/get_landsat.py --lon 31.1346 --lat 29.9793 -w 5000 -l 5000 -s 2015-04-05 -e 2015-07-20 -o tests/l8
python3 tsd/get_sentinel2.py --lon 31.1346 --lat 29.9793 -w 5000 -l 5000 -s 2016-04-05 -e 2016-05-12 -o tests/s2

# test the 8 (api, mirror) combinations
#for API in devseed planet scihub gcloud
#do
#    for MIRROR in aws gcloud
#    do
#        echo ${API} ${MIRROR}
#        python3 tsd/get_sentinel2.py --lon -102.5364 --lat 32.4396 -w 2560 -l 2560 -s 2016-04-05 -e 2016-07-20 -o tests/s2_${API}_${MIRROR} --api ${API} --mirror ${MIRROR}
#        echo
#        echo
#        echo
#    done
#done
