"""
This module contains parsers for the outputs of all the search API supported by
TSD, such as devseed, planet, scihub and gcloud. Each API parser receives as
input a Python dict containing the metadata of an image as returned by the API.
It extracts from it the metadata that TSD needs and stores it in an object
with standard attributes names (i.e. the attributes names are the same for all
API. The detailed list of the attributes is given below). This allows TSD to
use any search API with any download mirror.

Each parser returns an object with the following data structure:

    utm_zone (int): integer between 1 and 60 indicating the UTM longitude zone
    lat_band (str): letter between C and X, excluding I and O, indicating the
        UTM latitude band
    sqid (str): pair of letters indicating the MGRS 100x100 km square
    mgrd_id (str): concatenation of utm_zone, lat_band and sqid. It has lenght
        five (utm_zone is zero padded).
    date (datetime.datetime): acquisition date and time of the image
    satellite (str): either 'S2A' or 'S2B'
    orbit (int): relative orbit number
    title (str): original name of the SAFE in which the image is packaged by ESA
    is_old (bool): indicates wether or not the SAFE name follows the old (i.e. prior
        to 2016-12-06) or the new naming convention
    filename (str): string that TSD uses to name the crops downloaded for the bands
        of this image. It starts with the acquisition year, month and day so that
        sorting the files per image acquisition date is easy.
    urls (dict): dict with keys 'aws' and 'gcloud'. The value associated to
        each key is a dict with one key per band containing download urls.
    meta (dict): the original response of the API for this image
"""
from __future__ import print_function
import re
import pprint
import datetime
import dateutil.parser
import requests
import json
from bs4 import BeautifulSoup
from tsd.search_scihub import read_copernicus_credentials_from_environment_variables

AWS_S3_URL_L1C = 's3://sentinel-s2-l1c'
AWS_S3_URL_L2A = 's3://sentinel-s2-l2a'
AWS_HTTPS_URL_L8 = 'https://landsat-pds.s3.amazonaws.com'
GCLOUD_URL_L1C = 'https://storage.googleapis.com/gcp-public-data-sentinel-2'
GCLOUD_URL_LANDSAT = 'https://storage.googleapis.com/gcp-public-data-landsat'
SCIHUB_API_URL = "https://scihub.copernicus.eu/apihub/odata/v1"
RODA_URL = 'https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles'

ALL_BANDS = ['TCI', 'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08',
             'B8A', 'B09', 'B10', 'B11', 'B12']

ALL_BANDS_LANDSAT = ['B{}'.format(i) for i in range(1,12)] + ['BQA']


def get_granule_id_from_json(url):
    r = json.loads(requests.get(url).text)['datastrip']['id']
    return r.split('.')[0]

def get_granule_id_from_xml(url):
    r = requests.get(url).text
    soup = BeautifulSoup(r, 'lxml')
    return soup.find('mask_filename').text.split('/')[1]


def get_s2_granule_id_of_scihub_item_from_sentinelhub(img):
    """
    Build the granule id of a given single tile SAFE.

    The hard part is to get the timestamp in the granule id. Unfortunately this
    timestamp is not part of the metadata returned by scihub. This function queries
    sentinelhub to retrieve it. It takes about 3 seconds.

    Args:
        img (dict): single SAFE metadata as returned by scihub opensearch API

    Return:
        str: granule id, e.g. L1C_T36RTV_A005095_20180226T084545
    """
    import sentinelhub
    mgrs_id = img['tileid']
    orbit_number = img['orbitnumber']
    date = dateutil.parser.parse(img['beginposition'])

    t0 = (date - datetime.timedelta(hours=2)).isoformat()
    t1 = (date + datetime.timedelta(hours=2)).isoformat()
    r = sentinelhub.opensearch.get_tile_info('T{}'.format(mgrs_id), time=(t0, t1))
    assert(isinstance(r, dict))

    granule_date = dateutil.parser.parse(r['properties']['startDate']).strftime("%Y%m%dT%H%M%S")
    return "L1C_T{}_A{:06d}_{}".format(mgrs_id, orbit_number, granule_date)


def get_s2_granule_id_of_scihub_item_from_roda(img):
    """
    Build the granule id of a given single tile SAFE.

    The hard part is to get the timestamp in the granule id. Unfortunately this
    timestamp is not part of the metadata returned by scihub. This function queries
    roda to retrieve it. It takes about 200 ms.

    Args:
        img (dict): single SAFE metadata as returned by scihub opensearch API

    Return:
        str: granule id, e.g. L1C_T36RTV_A005095_20180226T084545
    """
    mgrs_id = img['tileid']
    orbit_number = img['orbitnumber']
    date = dateutil.parser.parse(img['beginposition'])

    utm_zone = mgrs_id[:2]
    lat_band = mgrs_id[2]
    sqid = mgrs_id[3:]

    roda_url = '{}/{}/{}/{}/{}/{}/{}/0/productInfo.json'.format(RODA_URL, utm_zone, lat_band, sqid,
                                                                date.year, date.month, date.day)
    r = requests.get(roda_url)
    if r.ok:
        d = r.json()
        granule_date = dateutil.parser.parse(d['tiles'][0]['timestamp']).strftime("%Y%m%dT%H%M%S")
    return "L1C_T{}_A{:06d}_{}".format(mgrs_id, orbit_number, granule_date)


def get_s2_granule_id_of_scihub_item_from_scihub(img):
    """
    Build the granule id of a given single tile SAFE.

    The hard part is to get the timestamp in the granule id. Unfortunately this
    timestamp is not part of the metadata returned by scihub. This function queries
    scihub OData API to retrieve it. It can be insanely slow (a few minutes).
    Another con is that it needs credentials.

    Args:
        img (dict): single SAFE metadata as returned by scihub opensearch API

    Return:
        str: granule id, e.g. L1C_T36RTV_A005095_20180226T084545
    """
    granule_request = "{}/Products('{}')/Nodes('{}')/Nodes('GRANULE')/Nodes?$format=json".format(SCIHUB_API_URL,
                                                                                                 img['id'],
                                                                                                 img['filename'])
    granules = requests.get(granule_request, auth=(read_copernicus_credentials_from_environment_variables())).json()
    return granules["d"]["results"][0]["Id"]


def band_resolution(b):
    """
    """
    if b in ['B02', 'B03', 'B04', 'B08', 'TCI']:
        return 10
    elif b in ['B05', 'B06', 'B07', 'B8a', 'B11', 'B12']:
        return 20
    elif b in ['B01', 'B09', 'B10']:
        return 60
    else:
        print('ERROR: {} is not in {}'.format(b, ALL_BANDS))

class LandsatGcloudParser:
    def __init__(self, img):
        self.meta = img
        self.urls = {'aws': {}, 'gcloud': {}}
        self._parse()
        self._build_gs_links()
        self._build_s3_links()
        self.filename = '{}_{}_{}_{}'.format(self.date.date().isoformat(),
                                             self.satellite,
                                             self.sensor,
                                             self.scene_id)

    def _parse(self):
        d = self.meta.copy()
        self.scene_id = d['scene_id']
        self.satellite = d['spacecraft_id'].replace('ANDSAT_','')
        self.sensor = d['sensor_id'].replace('_', '')
        self.date = dateutil.parser.parse(d['sensing_time'])
        self.row = d['wrs_row']
        self.path = d['wrs_path']

    def _build_gs_links(self):
        base_url = self.meta['base_url']
        scene_id_bis = base_url.strip('/').split('/')[-1]
        for band in ALL_BANDS_LANDSAT:
            self.urls['gcloud'][band] = '{}/{}_{}.TIF'.format(base_url, scene_id_bis, band)

    def _build_s3_links(self):
        pass

class LandsatDevSeedParser:
    def __init__(self, img):
        self.meta = img
        self.urls = {'aws': {}, 'gcloud': {}}
        self._parse()
        self._build_gs_links()
        self._build_s3_links()
        self.filename = '{}_{}_{}_{}'.format(self.date.date().isoformat(),
                                             self.satellite,
                                             self.sensor,
                                             self.scene_id)

    def _parse(self):
        d = self.meta.copy()
        self.scene_id = d['properties']['landsat:scene_id']
        self.product_id = d['properties']['landsat:product_id']
        self.satellite = d['properties']['eo:platform'].replace('andsat-','').upper()
        self.sensor = d['properties']['eo:instrument'].replace('_', '')
        self.date = dateutil.parser.parse(d['properties']['datetime'])
        self.row = d['properties']['eo:row']
        self.path = d['properties']['eo:column']

    def _build_gs_links(self):
        d = self.meta.copy()
        for band in ALL_BANDS_LANDSAT:
            sat_bis,*_,collec,_ = self.product_id.split('_')
            base_url = '{}/{}/{}/{}/{}/{}'.format(GCLOUD_URL_LANDSAT,sat_bis,collec,self.path,self.row, self.product_id)
            self.urls['gcloud'][band] = '{}/{}_{}.TIF'.format(base_url, self.product_id, band)

    def _build_s3_links(self):
        if self.satellite!='L8':
            return
        d = self.meta.copy()
        for band in ALL_BANDS_LANDSAT:
            band_dico = d['assets'].get(band)
            if band_dico is not None:
                self.urls['aws'][band] = band_dico['href']


class DevSeedParser:
    def __repr__(self):
        return pprint.pformat(self.__dict__)

    def __init__(self, img):
        self.meta = img
        self.urls = {'aws': {}, 'gcloud': {}}
        self._parse()
        self._build_gs_links()
        self._build_s3_links()
        self.filename = '{}_{}_orbit_{:03d}_tile_{}'.format(self.date.date().isoformat(),
                                                            self.satellite, self.orbit,
                                                            self.mgrs_id)

    def _parse(self):
        d = self.meta.copy()
        self.utm_zone  = d['properties']['sentinel:utm_zone']
        self.lat_band  = d['properties']['sentinel:latitude_band']
        self.sqid  = d['properties']['sentinel:grid_square']
        self.mgrs_id = '{}{}{}'.format(self.utm_zone, self.lat_band, self.sqid)
        self.date = dateutil.parser.parse(d['properties']['datetime'])
        self.title = d['properties']['sentinel:product_id']
        self.id = d['id']
        self.granule_id = get_granule_id_from_xml(d['assets']['metadata']['href'])

        s = re.search('_R([0-9]{3})_', self.title)
        self.orbit = int(s.group(1)) if s else 0
        self.satellite = d['properties']['eo:platform'].replace("sentinel-", "S")  # Sentinel-2B --> S2B
        self.is_old = True if 'OPER' in self.title else False
        self.cloud_cover = d['properties']['eo:cloud_cover']
        self.thumbnail = d['assets']['thumbnail']['href'].replace('sentinel-s2-l1c.s3.amazonaws.com',
                                                                  'roda.sentinel-hub.com/sentinel-s2-l1c')


    def _build_gs_links(self):
        if self.is_old:  # old safes, before 2016-12-6
            _, _, _, msi, _, d1, r, v, d2 = self.title.split('_')
            _, _, _, _, _, _, _, d3, a, t, n = self.id.split('_')
            safe_name = '_'.join([self.satellite, msi, v[1:], n.replace('.', ''), r, t, d1])
            # safe_name = self.title
            _, _, d1, _, _, t, _ = safe_name.split('_')
            img_name = '{}_{}_{}.jp2'.format(t, d1, '{}')
            cloud_mask_name = '{}_B00_MSIL1C.gml'.format('_'.join(self.id.split('_')[:-1]).replace('MSI_L1C_TL', 'MSK_CLOUDS'))

        else:
            safe_name = self.title
            _, _, d1, _, r, t, d2 = safe_name.split('_')
            img_name = '{}_{}_{}.jp2'.format(t, d1, '{}')
            cloud_mask_name = 'MSK_CLOUDS_B00.gml'

        granule_id = self.granule_id
        base_url = '{}/tiles/{}/{}/{}/{}.SAFE'.format(GCLOUD_URL_L1C,
                                                      self.utm_zone,
                                                      self.lat_band,
                                                      self.sqid,
                                                      safe_name)
        full_url = '{}/GRANULE/{}/IMG_DATA/{}'.format(base_url, granule_id, img_name)
        for band in ALL_BANDS:
            self.urls['gcloud'][band] = full_url.format(band)
        self.urls['gcloud']['cloud_mask'] = '{}/GRANULE/{}/QI_DATA/{}'.format(base_url, granule_id, cloud_mask_name)

    def _build_s3_links(self):
        # d = self.meta.copy()
        # for band in ALL_BANDS:
        #     href =d['assets'][band][href]
        #     if 'l1c' in href:
        #         href = href.replace('https://sentinel-s2-l1c.s3.amazonaws.com', AWS_S3_URL_L1C)
        #     else:
        #         href = href.replace('https://sentinel-s2-l2a.s3.amazonaws.com', AWS_S3_URL_L2A)
        #     self.urls['aws'][band] = href
        aws_s3_url = AWS_S3_URL_L2A if 'MSIL2A' in self.title else AWS_S3_URL_L1C
        base_url = '{}/tiles/{}/{}/{}/{}/{}/{}/0'.format(aws_s3_url, self.utm_zone, self.lat_band, self.sqid,
                                                         self.date.year, self.date.month, self.date.day)
        full_url = '{}/R{}m/{}.jp2'.format(base_url, '{}', '{}') if 'MSIL2A' in self.title else '{}/{}.jp2'.format(base_url, '{}')
        for band in ALL_BANDS:
            self.urls['aws'][band] = full_url.format(band) if 'MLSL2A' not in self.title else full_url.format(band_resolution(band), band)
        self.urls['aws']['cloud_mask'] = '{}/qi/MSK_CLOUDS_B00.gml'.format(base_url)

class GcloudParser:
    def __init__(self, img):
        self.meta = img
        self.urls = {'aws': {}, 'gcloud': {}}
        self._parse()
        self._build_gs_links()
        self._build_s3_links()
        self.filename = '{}_{}_orbit_{:03d}_tile_{}'.format(self.date.date().isoformat(),
                                                            self.satellite, self.orbit,
                                                            self.mgrs_id)

    def _parse(self):
        d = self.meta.copy()
        self.date = dateutil.parser.parse(d['sensing_time'], ignoretz=True)
        self.mgrs_id = d['mgrs_tile']
        self.utm_zone, self.lat_band, self.sqid = re.split('(\d+)([a-zA-Z])([a-zA-Z]+)', self.mgrs_id)[1:4]
        self.is_old = True if '.' in d['granule_id'] else False
        self.orbit = int(d['product_id'].split('_')[6][1:]
                         if self.is_old else d['product_id'].split('_')[4][1:])
        self.satellite = d['product_id'][:3]
        self.title = d['product_id']

    def _build_gs_links(self):
        safe_name = None
        base_url = self.meta['base_url']
        granule_id = self.meta['granule_id']
        if self.is_old:
            img_name = '{}_{}.jp2'.format('_'.join(granule_id.split('_')[:-1]), '{}')
            cloud_mask_name = '{}_B00_MSIL1C.gml'.format('_'.join(granule_id.split('_')[:-1]).replace('MSI_L1C_TL', 'MSK_CLOUDS'))
        else:
            d1 = self.meta['product_id'].split('_')[2]
            img_name = 'T{}_{}_{}.jp2'.format(self.mgrs_id, d1, '{}')
            cloud_mask_name = 'MSK_CLOUDS_B00.gml'

        full_url = '{}/GRANULE/{}/IMG_DATA/{}'.format(base_url, granule_id, img_name)
        for band in ALL_BANDS:
            self.urls['gcloud'][band] = full_url.format(band)
        self.urls['gcloud']['cloud_mask'] = '{}/GRANULE/{}/QI_DATA/{}'.format(base_url, granule_id, cloud_mask_name)

    def _build_s3_links(self):
        aws_s3_url = AWS_S3_URL_L1C
        base_url = '{}/tiles/{}/{}/{}/{}/{}/{}/0'.format(aws_s3_url, self.utm_zone, self.lat_band, self.sqid,
                                                         self.date.year, self.date.month, self.date.day)

        full_url = '{}/{}.jp2'.format(base_url, '{}')
        for band in ALL_BANDS:
            self.urls['aws'][band] = full_url.format(band)
        self.urls['aws']['cloud_mask'] = '{}/qi/MSK_CLOUDS_B00.gml'.format(base_url)


class PlanetParser:
    def __init__(self, img):
        self.meta = img
        self.urls = {'aws': {}, 'gcloud': {}}
        self._parse()
        self._build_gs_links()
        self._build_s3_links()
        self.filename = '{}_{}_orbit_{:03d}_tile_{}'.format(self.date.date().isoformat(),
                                                            self.satellite, self.orbit,
                                                            self.mgrs_id)

    def _parse(self):
        d = self.meta.copy()
        self.mgrs_id = d['properties']['mgrs_grid_id']
        self.utm_zone, self.lat_band, self.sqid = re.split(
            '(\d+)([a-zA-Z])([a-zA-Z]+)', self.mgrs_id)[1:4]
        self.date = dateutil.parser.parse(d['properties']['acquired'])
        self.orbit = d['properties']['rel_orbit_number']
        self.satellite = d['properties']['satellite_id'].replace("Sentinel-", "S")  # Sentinel-2A --> S2A
        self.title = d['id']

    def _build_gs_links(self):
        pass

    def _build_s3_links(self):
        aws_s3_url = AWS_S3_URL_L1C
        base_url = '{}/tiles/{}/{}/{}/{}/{}/{}/0'.format(aws_s3_url, self.utm_zone, self.lat_band, self.sqid,
                                                         self.date.year, self.date.month, self.date.day)
        full_url = '{}/{}.jp2'.format(base_url, '{}')
        for band in ALL_BANDS:
            self.urls['aws'][band] = full_url.format(band)
        self.urls['aws']['cloud_mask'] = '{}/qi/MSK_CLOUDS_B00.gml'.format(base_url)


class SciHubParser:
    def __init__(self, img):
        self.meta = img
        self.urls = {'aws': {}, 'gcloud': {}}
        self._parse()
        self._build_gs_links()
        self._build_s3_links()
        self.filename = '{}_{}_orbit_{:03d}_tile_{}'.format(self.date.date().isoformat(),
                                                            self.satellite, self.orbit,
                                                            self.mgrs_id)

    def _parse(self):
        d = self.meta.copy()
        self.date = dateutil.parser.parse(d["beginposition"], ignoretz=True)
        if "tileid" not in d:
            self.mgrs_id = re.findall(r"_T([0-9]{2}[A-Z]{3})_", d['title'])[0]
        else:
            self.mgrs_id = d["tileid"]
        self.utm_zone, self.lat_band, self.sqid = re.split('(\d+)([a-zA-Z])([a-zA-Z]+)', self.mgrs_id)[1:4]
        s = re.search('_R([0-9]{3})_', d['title'])
        self.orbit = int(s.group(1)) if s else 0
        self.satellite = d['title'][:3]  # S2A_MSIL1C_2018010... --> S2A
        self.title = d['title']
        self.name = d['filename']

    def _build_gs_links(self):
        """
        Example:
        https://storage.googleapis.com/gcp-public-data-sentinel-2/tiles/36/R/TV/S2B_MSIL1C_20180226T083909_N0206_R064_T36RTV_20180226T122942.SAFE/GRANULE/L1C_T36RTV_A005095_20180226T084545/IMG_DATA/T36RTV_20180226T083909_B01.jp2

        The tricky part is to find the time (084545 in the example above) in
        the granule name, which is not part of the scihub API response.
        """
        base_url = '{}/tiles/{}/{}/{}/{}.SAFE'.format(GCLOUD_URL_L1C,
                                                      self.utm_zone,
                                                      self.lat_band,
                                                      self.sqid,
                                                      self.title)

        granule = get_s2_granule_id_of_scihub_item_from_roda(self.meta)
        full_url = '{}/GRANULE/{}'.format(base_url, granule)

        self.urls['gcloud']['cloud_mask'] = '{}/QI_DATA/MSK_CLOUDS_B00.gml'.format(full_url)
        date = self.title.split('_')[2]
        for band in ALL_BANDS:
            self.urls['gcloud'][band] = '{}/IMG_DATA/T{}_{}_{}.jp2'.format(full_url, self.mgrs_id, date, band)


    def _build_s3_links(self):
        aws_s3_url = AWS_S3_URL_L1C
        base_url = '{}/tiles/{}/{}/{}/{}/{}/{}/0'.format(aws_s3_url, self.utm_zone, self.lat_band, self.sqid,
                                                         self.date.year, self.date.month, self.date.day)
        full_url = '{}/{}.jp2'.format(base_url, '{}')
        for band in ALL_BANDS:
            self.urls['aws'][band] = full_url.format(band)
        self.urls['aws']['cloud_mask'] = '{}/qi/MSK_CLOUDS_B00.gml'.format(base_url)
