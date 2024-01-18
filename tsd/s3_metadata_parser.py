"""
This module contains parsers for the Sentinel-3 metadata outputs of all the
search APIs supported by TSD, so only scihub at the moment. Each
API parser receives as input a Python dict containing the metadata of an image
as returned by the API. It extracts from it the metadata that TSD needs and
stores them in an object with standard attributes (i.e. the attributes are the
same for all APIs). The detailed list of attributes is given below. This allows
TSD to use any search API with any download mirror.

Each parser returns a Sentinel3Image object with the following attributes:

    date (datetime.datetime): acquisition date and time of the image
    satellite (str): either 'S3A' or 'S3B'
    orbit (int): relative orbit number
    title (str): original name of the SAFE in which the image is packaged by ESA
    filename (str): string that TSD uses to name the crops downloaded for the bands
        of this image. It starts with the acquisition year, month and day so that
        sorting the files per image acquisition date is easy.
    urls (dict): dict with keys 'aws' and 'gcloud'. The value associated to
        each key is a dict with one key per band containing download urls.
    metadata_original (dict): the original response of the API for this image
"""
import re
import json
import datetime
import geojson

import dateutil.parser
import requests
import shapely
import xmltodict

from tsd import utils

AWS_S3_URL_COGS = 's3://meeo-s3-cog'
SCIHUB_API_URL = 'https://scihub.copernicus.eu/apihub/odata/v1'

BANDS_L1 = ['S1', 'S2', 'S3', 'S4', 'S5', 'S6']

BANDS_RESOLUTION = {'S1': 500,
                    'S2': 500,
                    'S3': 500,
                    'S4': 500,
                    'S5': 500,
                    'S6': 500}

# Correspondence between band name and band index, from page 57 / 496 of
# https://sentinel.esa.int/documents/247904/349490/S2_MSI_Product_Specification.pdf
BANDS_INDEX = {'0': 'S1',
               '1': 'S2',
               '2': 'S3',
               '3': 'S4',
               '4': 'S5',
               '5': 'S6'}


def parse_safe_name_for_acquisition_date(safe_name):
    """
    Parse a SAFE name for the corresponding acquisition date.

    Example of a SAFE name:
        S2A_MSIL1C_20180105T185751_N0206_R113_T10SEG_20180105T204427 --> 20180105T185751
    """
    date_str = re.findall(r"_(2[0-9]{3}[0-1][0-9][0-3][0-9]T[0-9]{6})_",
                          safe_name)[0]
    return dateutil.parser.parse(date_str, ignoretz=True)


def parse_safe_name_for_product_type(safe_name):
    """
    Parse a SAFE name for the corresponding product type,

    Example of a SAFE name:
        S3B_SL_1_RBT____20221005T235626_20221005T235926_20221007T001905_0179_071_173_1620_PS2_O_NT_004 -> SL_1_RBT___
    """
    return re.findall(r"SL_[0-9]_[A-Z]{3}___", safe_name)[0]

class Sentinel3Image(dict):
    """
    Sentinel-3 image metadata class.
    """
    # use dict setters and getters, so that object interaction is like a dict
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, img, cloud=None, api='scihub'):
        """
        """
        self.metadata_source = api

        if api == 'scihub':
            self.scihub_parser(img)
        if cloud:
            self.title_cloud = cloud['title']

        self.date = parse_safe_name_for_acquisition_date(self.title)
        self.processing_level = parse_safe_name_for_product_type(self.title)
        self.satellite = self.title[:3]  # S3B_SL_1_RBT____20221005T193828... --> S3B
        self.delivery_time = 'NTC' # FIXME Allow different delivery times

        self.filename = self.title
        self.urls = {'aws': {}}

    def scihub_parser(self, img):
        """
        Args:
            img (dict): json metadata dict for a single SAFE, as shipped in scihub
                opensearch API response
        """
        self.title = img['title']
        self.absolute_orbit = img['orbitnumber']
        self.thumbnail = img['links']['icon']
        s = img["footprint"]
        self.geometry = geojson.Feature(geometry=shapely.wkt.loads(s))["geometry"]

    def build_s3_links(self):
        """
        Build s3 urls for all raster bands and the cloud mask.

        Examples of urls:
            L1: s3://meeo-s3-cog/NTC/S3A/SL_1_RBT___/2022/08/07/S3A_SL_1_RBT____20220807T235435_20220807T235735_20220809T083053_0179_088_244_3420_PS1_O_NT_004_S5_radiance_an.tif
        """
        urls = self.urls['aws']

        assert self.processing_level == "SL_1_RBT___", "Only SL_1_RBT___ is currently supported"

        base_url = "{}/{}/{}/{}/{}/{:02d}/{:02d}/{}".format(AWS_S3_URL_COGS,
                                                    self.delivery_time,
                                                    self.satellite,
                                                    self.processing_level,
                                                    self.date.year,
                                                    self.date.month,
                                                    self.date.day,
                                                    self.title)

        urls["cloud_mask"] = "{}/{}/{}/{}/{}/{:02d}/{:02d}/{}_cloud_in.tif".format(AWS_S3_URL_COGS,
                                                    self.delivery_time,
                                                    self.satellite,
                                                    "SL_2_LST___",
                                                    self.date.year,
                                                    self.date.month,
                                                    self.date.day,
                                                    self.title_cloud)
        ext = "tif"
        # FIXME Change this by something more generic
        bands = BANDS_L1
        for b in bands:
            urls[b] = "{}_{}.{}".format(base_url, b + "_radiance_an", ext)


    def get_satellite_angles(self):
        assert 0 == 1, "Satellite angles not yet supported"
