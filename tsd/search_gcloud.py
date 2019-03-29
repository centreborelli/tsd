import os
import argparse
import datetime
import json
import requests
import shapely.geometry
import numpy as np
import pandas as pd

# from pandas.io import gbq
from google.cloud import bigquery

from bs4 import BeautifulSoup
from tqdm import tqdm
import utils

from pyproj import Proj, transform
from json.decoder import JSONDecodeError


def parse_url(url):
    _, file = url.split('gs://')
    bucket, *prefix = file.split('/')
    return bucket, '/'.join(prefix), prefix[-1]


def get_footprint(img, source='roda'):
    mgrs = img['mgrs_tile']
    date = pd.to_datetime(img['sensing_time'])

    if source=='roda':
        url = 'https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/{}/{}/{}/{}/{}/{}/0/tileInfo.json'.format(mgrs[:2].lstrip('0'),
                                                                                                             mgrs[2],
                                                                                                             mgrs[3:],
                                                                                                             date.year,
                                                                                                             date.month,
                                                                                                             date.day)
        try:
            metadata = requests.get(url).json()
        except JSONDecodeError:
            return get_footprint(img, source='google')

        key = 'tileDataGeometry' if 'tileDataGeometry' in metadata else 'tileGeometry'
        epsg = metadata[key]['crs']['properties']['name'].split(':')[-1]
        utm_coords = metadata[key]['coordinates'][0]

    else:
        # Source should be google
        if '.' in img['granule_id']:
            # Old format
            filename = '{}.xml'.format(img['product_id'].replace('PRD_MSIL1C', 'MTD_SAFL1C'))
        else:
            # New format
            filename = 'MTD_MSIL1C.xml'

        url = '{}/{}'.format(img['base_url'].replace('gs://', 'http://storage.googleapis.com/'), filename)
        r = requests.get(url)
        coords = BeautifulSoup(r.content, 'lxml').find('ext_pos_list').text.strip().split(' ')
        coords = [(float(coords[2*i]),float(coords[2*i+1])) for i in range(int(len(coords)/2))]
        ref_lat, ref_lon = coords[0]
        _,_,zn,_ = utm.from_latlon(ref_lat, ref_lon)
        utm_coords = [utm.from_latlon(x,y)[:2] for x,y in coords]
        epsg = str(32700-round((45+ref_lat)/90)*100+zn)

    return shapely.geometry.Polygon(utm_coords), epsg


def convert_aoi_to_utm(aoi, epsg):
    outProj = Proj(init='epsg:{}'.format(epsg))
    inProj = Proj(init='epsg:4326')
    utm_aoi = []
    for x, y in aoi['coordinates'][0]:
        utm_aoi.append(transform(inProj, outProj, x, y))
    return shapely.geometry.Polygon(utm_aoi)


def query_string(lat, lon, start_date, end_date, satellite, sensor):
    if end_date is None:
        end_date = datetime.date.today()
    if start_date is None:
        start_date = end_date - datetime.timedelta(365)

    date_query = 'sensing_time >= "{}" AND sensing_time <= "{}"'.format(start_date, end_date)
    loc_query = 'north_lat>={} AND south_lat<={} AND west_lon<={} AND east_lon>={}'.format(lat, lat, lon, lon)
    additional_query = ''

    if satellite=='Sentinel-2':
        tab_name = '`bigquery-public-data.cloud_storage_geo_index.sentinel_2_index`'
    elif 'Landsat' in satellite:
        tab_name = '`bigquery-public-data.cloud_storage_geo_index.landsat_index`'
        if sensor is not None:
            sensor_query = ' AND sensor_id="{}"'.format(sensor.replace('OLITIRS', 'OLI_TIRS'))
            additional_query += sensor_query
        if '-' in satellite:
            # Specific query for one Landsat
            sat_query = ' AND spacecraft_id="{}"'.format(satellite.upper().replace('-', '_'))
            additional_query += sat_query
    else:
        raise KeyError('Wrong Satellite name, you entered {}'.format(satellite))

    query = 'SELECT * FROM {} WHERE {} AND {}{}'.format(tab_name, date_query, loc_query, additional_query)

    return query


def search(aoi, start_date=None, end_date=None, satellite='Sentinel-2', sensor=None):
    """
    List images covering an area of interest (AOI) using Google Index.

    Args:
        aoi: geojson.Polygon or geojson.Point object
    """
    # compute the centroid of the area of interest
    lon, lat = shapely.geometry.shape(aoi).centroid.coords.xy
    lon, lat = lon[0], lat[0]

    # build query
    query = query_string(lat, lon, start_date, end_date, satellite, sensor)

    # query Gcloud BigQuery Index
    try:
        private_key = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except KeyError as e:
        print('You must have the env variable GOOGLE_APPLICATION_CREDENTIALS linking to the cred json file')
        raise e

    # df = gbq.read_gbq(query, private_key=private_key)
    client = bigquery.Client.from_service_account_json(private_key)
    rows = list(client.query(query).result())
    df = pd.DataFrame(dict(row.items()) for row in rows)

    # check if the image footprint contains the area of interest
    if satellite=='Sentinel-2':
        res = []
        for i, row in df.iterrows():
            footprint, epsg = get_footprint(row)
            utm_aoi = convert_aoi_to_utm(aoi, epsg)
            if footprint.contains(utm_aoi):
                res.append(row.to_dict())
    else:
        # We need to remove duplicates
        order_collection_category = {'T1':0, 'T2':1, 'T3':2, 'RT':3, 'N/A':4}
        order_collection_number = {'01':0, 'PRE':1}
        df['order_collection_category'] = df['collection_category'].apply(lambda x: order_collection_category[x])
        df['order_collection_number'] = df['collection_number'].apply(lambda x: order_collection_number[x])
        unique_scene = ['wrs_path', 'wrs_row', 'spacecraft_id', 'sensor_id', 'date_acquired']
        orders = ['order_collection_number', 'order_collection_category']
        df.sort_values(by=unique_scene+orders, inplace=True)
        res = df.groupby(unique_scene).first().reset_index().drop(orders, axis=1).sort_values(by=['date_acquired']).to_dict('records')
    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Search of Sentinel-2 images.')
    parser.add_argument('--satellite', choices=['Sentinel-2', 'Landsat', 'Landsat-4', 'Landsat-5', 'Landsat-6', 'Landsat-7', 'Landsat-8'],
                        help=('either all "Landsat", one specific "Landsat-8" or "Sentinel-2"'),
                        default='Sentinel-2')
    parser.add_argument('--sensor', choices=['MSS', 'TM', 'ETM', 'OLITIRS'],
                        help=('Only for Landsat, see https://landsat.usgs.gov/what-are-band-designations-landsat-satellites'),
                        default=None)
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
                            end_date=args.end_date, satellite=args.satellite,
                            sensor=args.sensor)))
