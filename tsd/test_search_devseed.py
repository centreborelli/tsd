import datetime

from tsd import utils
from tsd import search_devseed

aoi = utils.geojson_geometry_object(29.9793, 31.1346, 5000, 5000)
results = search_devseed.search(aoi,
                                start_date=datetime.datetime(2019, 1, 15),
                                end_date=datetime.datetime(2019, 1, 30),
                                satellite='Sentinel-2')

expected_titles = ['S2B_MSIL1C_20190129T083209_N0207_R021_T36RUU_20190129T103220',
                   'S2A_MSIL1C_20190124T083231_N0207_R021_T36RUU_20190124T095836',
                   'S2B_MSIL1C_20190119T083259_N0207_R021_T36RUU_20190119T104924']
assert([r['properties']['sentinel:product_id'] for r in results] == expected_titles)
