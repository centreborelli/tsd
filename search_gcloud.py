import os
import argparse
import datetime
import json
import requests
import shapely.geometry
import numpy as np
import pandas as pd

import pandas_gbq as gbq
from google.cloud import storage
from bs4 import BeautifulSoup
from tqdm import tqdm
import utils

from pyproj import Proj, transform
from json.decoder import JSONDecodeError

def parse_url(url):
    _, file = url.split('gs://')
    bucket, *prefix = file.split('/')
    return bucket, '/'.join(prefix), prefix[-1]

def get_footprint(img):
    mgrs = img['mgrs_tile']
    date = pd.to_datetime(img['sensing_time'])
    url = 'https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/{}/{}/{}/{}/{}/{}/0/tileInfo.json'.format(mgrs[:2],
                                                                                                         mgrs[2],
                                                                                                         mgrs[3:],
                                                                                                         date.year,
                                                                                                         date.month,
                                                                                                         date.day)
    try:
        metadata = requests.get(url).json()
    except JSONDecodeError:
        return get_footprint_gcloud(img)

    epsg = metadata['tileDataGeometry']['crs']['properties']['name'].split(':')[-1]
    inProj = Proj(init='epsg:{}'.format(epsg))
    outProj = Proj(init='epsg:4326')
    coords = []
    for x,y in metadata['tileDataGeometry']['coordinates'][0]:
        coords.append(transform(inProj,outProj,x,y))
    poly = shapely.geometry.Polygon(coords)
    return poly

def get_footprint_gcloud(img):
    bucket_name, prefix, _ = parse_url(img['base_url'])
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    r = bucket.list_blobs(prefix=prefix)
    for hit in r:
        name = hit.name
        if name.endswith('.xml') and len(name.split('/'))==6:
            if not 'inspire' in name.lower() and not 'manifest' in name.lower():
                blob_name = name
                break

    r = bucket.get_blob(blob_name)
    soup = BeautifulSoup(r.download_as_string(), 'lxml')
    coords = soup.find('global_footprint').find('ext_pos_list').text.strip().split(' ')
    coords = [(' '.join((coords[2*i+1], coords[2*i]))) for i in range(int(len(coords)/2))]
    coords = [c.split(' ') for c in coords]
    coords = [(float(a), float(b)) for a,b in coords]
    poly = shapely.geometry.Polygon(coords)
    return poly


def query_s2(lat, lon, start_date, end_date):
    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)

    tab_name = '[bigquery-public-data:cloud_storage_geo_index.sentinel_2_index]'
    date_query = 'sensing_time >= "{}" AND sensing_time <= "{}"'.format(start_date, end_date)
    loc_query = 'north_lat>={} AND south_lat<={} AND west_lon<={} AND east_lon>={}'.format(lat, lat, lon, lon)
    query = 'SELECT * FROM {} WHERE {} AND {}'.format(tab_name, date_query, loc_query)

    return query

def search(aoi, start_date=None, end_date=None):
    """
    List images covering an area of interest (AOI) using Google Index.

    Args:
        aoi: geojson.Polygon or geojson.Point object
    """
    # compute the centroid of the area of interest
    lon, lat = shapely.geometry.shape(aoi).centroid.coords.xy
    lon, lat = lon[0], lat[0]

    # build query
    search_string = query_s2(lat, lon, start_date, end_date)

    # query Gcloud BigQuery Index
    try:
        private_key = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except KeyError:
        print('You must have the env variable GOOGLE_APPLICATION_CREDENTIALS linking to the cred json file')
        raise KeyError
    print('Looking for images...')
    df = gbq.read_gbq(search_string, private_key=private_key)
    print('{} images found.'.format(len(df)))

    # check if the image footprint contains the area of interest
    aoi = shapely.geometry.shape(aoi)
    res = []
    print('Checking that the images contain the aoi...')
    for i, row in tqdm(df.iterrows(), total=len(df)):
        print(row)
        print('\n\n')
        print(row['base_url'])
        print('\n\n')
        poly = get_footprint(row)
        if poly.contains(aoi):
            res.append(row.to_dict())
    print('There are {} images that contain the aoi.'.format(len(res)))
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search of Landsat-8 and Sentinel-2 images.')
    parser.add_argument('--geom', type=utils.valid_geojson,
                        help=('path to geojson file'))
    parser.add_argument('--lat', type=utils.valid_lat,
                        help=('latitude of the center of the rectangle AOI'))
    parser.add_argument('--lon', type=utils.valid_lon,
                        help=('longitude of the center of the rectangle AOI'))
    parser.add_argument('-w', '--width', type=int, default=5000,
                        help='width of the AOI (m), default 5000 m')
    parser.add_argument('-l', '--height', type=int, default=5000,
                        help='height of the AOI (m), default 5000 m')
    parser.add_argument('-s', '--start-date', type=utils.valid_datetime,
                        help='start date, YYYY-MM-DD')
    parser.add_argument('-e', '--end-date', type=utils.valid_datetime,
                        help='end date, YYYY-MM-DD')
    args = parser.parse_args()

    if args.geom and (args.lat or args.lon):
        parser.error('--geom and {--lat, --lon} are mutually exclusive')

    if not args.geom and (not args.lat or not args.lon):
        parser.error('either --geom or {--lat, --lon} must be defined')

    if args.geom:
        aoi = args.geom
    else:
        aoi = utils.geojson_geometry_object(args.lat, args.lon, args.width,
                                            args.height)

    print(json.dumps(search(aoi, start_date=args.start_date,
                            end_date=args.end_date)))
