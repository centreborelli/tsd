#!/bin/bash

set -e  # exit on errors. http://mywiki.wooledge.org/BashFAQ/105

mkdir -p tests

echo "Direct downloads..."
GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR CPL_VSIL_CURL_ALLOWED_EXTENSIONS=TIF rio clip http://landsat-pds.s3.amazonaws.com/c1/L8/176/039/LC08_L1TP_176039_20180614_20180703_01_T1/LC08_L1TP_176039_20180614_20180703_01_T1_B8.TIF tests/l8_rio.tif --bounds "314887.5 3312847.5 325177.5 3323107.5" --overwrite

GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR CPL_VSIL_CURL_ALLOWED_EXTENSIONS=jp2 rio clip http://storage.googleapis.com/gcp-public-data-sentinel-2/tiles/36/R/UU/S2A_MSIL1C_20180728T082601_N0206_R021_T36RUU_20180728T104559.SAFE/GRANULE/L1C_T36RUU_A016177_20180728T084109/IMG_DATA/T36RUU_20180728T082601_B04.jp2 tests/s2_rio.tif --bounds "314880 3312840 325170 3323100" --overwrite

LON=31.1346
LAT=29.9793
START="2018-04-05"
END="2018-04-20"
SIZE=2560

# test Landsat-8
echo
echo "Landsat-8 downloads..."
for API in stac #gcloud planet
do
   for MIRROR in gcloud aws
   do
       echo
       echo ${API} ${MIRROR}
       python3 tsd/get_landsat.py --lon ${LON} --lat ${LAT} -w ${SIZE} -l ${SIZE} -s ${START} -e ${END} --satellite Landsat-8 -o tests/l8_${API}_${MIRROR} --api ${API} --mirror ${MIRROR}
       echo
    done
done

# test the 8 (api, mirror) combinations for Sentinel-2
echo
echo "Sentinel-2 downloads..."
for API in stac scihub planet #gcloud
do
   for MIRROR in gcloud aws
   do
       echo
       echo ${API} ${MIRROR}
       python3 tsd/get_sentinel2.py --lon ${LON} --lat ${LAT} -w ${SIZE} -l ${SIZE} -s ${START} -e ${END} -o tests/s2_${API}_${MIRROR} --api ${API} --mirror ${MIRROR} --satellite-angles
       echo
   done
done
