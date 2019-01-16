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
import dateutil.parser
import datetime

AWS_S3_URL_L1C = 's3://sentinel-s2-l1c'
AWS_S3_URL_L2A = 's3://sentinel-s2-l2a'
GCLOUD_URL_L1C = 'gs://gcp-public-data-sentinel-2'

ALL_BANDS = ['TCI', 'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08',
             'B8A', 'B09', 'B10', 'B11', 'B12']


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
        mgrs_id = re.findall(r"_T([0-9]{2}[A-Z]{3})_", d['properties']['id'])[0]
        self.utm_zone, self.lat_band, self.sqid = mgrs_id[:2], mgrs_id[2], mgrs_id[3:]
        self.mgrs_id = mgrs_id
        self.date = dateutil.parser.parse(d['properties']['datetime'])
        self.title = d['properties']['sentinel:product_id']
        self.id = d['properties']['id']
        s = re.search('_R([0-9]{3})_', self.title)
        self.orbit = int(s.group(1)) if s else 0
        self.satellite = d['properties']['eo:platform'].replace("Sentinel-", "S")  # Sentinel-2B --> S2B
        self.is_old = True if 'OPER' in self.title else False

    def _build_gs_links(self):
        if self.is_old:  # old safes, before 2016-12-6
            _, _, _, msi, _, d1, r, v, d2 = self.title.split('_')
            _, _, _, _, _, _, _, d3, a, t, n = self.id.split('_')
            safe_name = '_'.join([self.satellite, msi, v[1:], n.replace('.', ''), r, t, d1])
            img_name = '{}_{}.jp2'.format('_'.join(self.id.split('_')[:-1]), '{}')
            cloud_mask_name = '{}_B00_MSIL1C.gml'.format('_'.join(self.id.split('_')[:-1]).replace('MSI_L1C_TL', 'MSK_CLOUDS'))

        else:
            safe_name = self.title
            _, _, d1, _, r, t, d2 = self.title.split('_')
            img_name = '{}_{}_{}.jp2'.format(t, d1, '{}')
            cloud_mask_name = 'MSK_CLOUDS_B00.gml'

        granule_id = self.id
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
        date_string = [a['content']
                       for a in d['date'] if a['name'] == 'beginposition'][0]
        self.date = dateutil.parser.parse(date_string, ignoretz=True)
        if self.date > datetime.datetime(2016, 8, 26):
            self.mgrs_id = re.findall(r"_T([0-9]{2}[A-Z]{3})_", d['title'])[0]
        else:
            print('ERROR: scihub API cannot be used for Sentinel-2 images older than 2016-8-26')
        self.utm_zone, self.lat_band, self.sqid = re.split('(\d+)([a-zA-Z])([a-zA-Z]+)', self.mgrs_id)[1:4]
        self.orbit = int(d['int'][1]['content'])
        self.satellite = d['title'][:3]  # S2A_MSIL1C_2018010... --> S2A
        self.title = d['title']

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
