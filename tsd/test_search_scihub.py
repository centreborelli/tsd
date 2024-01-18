import datetime

from tsd import search_scihub
from tsd import utils

aoi = utils.geojson_geometry_object(29.9793, 31.1346, 5000, 5000)
results = search_scihub.search(aoi=aoi,
                               start_date=datetime.datetime(2019, 1, 1),
                               end_date=datetime.datetime(2019, 1, 15),
                               satellite='Sentinel-2')
expected_titles = {'S2A_MSIL1C_20190114T083311_N0207_R021_T36RUU_20190114T085705.SAFE',
                   'S2B_MSIL1C_20190109T083329_N0207_R021_T36RUU_20190109T103019.SAFE',
                   'S2A_MSIL1C_20190104T083331_N0207_R021_T36RUU_20190104T104619.SAFE'}
assert expected_titles.issubset({r['Name'] for r in results})
