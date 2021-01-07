"""
This module contains parsers for the Sentinel-2 metadata outputs of all the
search APIs supported by TSD, such as stac, planet, scihub and gcloud. Each
API parser receives as input a Python dict containing the metadata of an image
as returned by the API. It extracts from it the metadata that TSD needs and
stores them in an object with standard attributes (i.e. the attributes are the
same for all APIs). The detailed list of attributes is given below. This allows
TSD to use any search API with any download mirror.

Each parser returns a Sentinel2Image object with the following attributes:

    utm_zone (int): integer between 1 and 60 indicating the UTM longitude zone
    lat_band (str): letter between C and X, excluding I and O, indicating the
        UTM latitude band
    sqid (str): pair of letters indicating the MGRS 100x100 km square
    mgrs_id (str): concatenation of utm_zone, lat_band and sqid. It has length
        five (utm_zone is zero padded).
    epsg (int): integer indicating the EPSG code of the image CRS
    date (datetime.datetime): acquisition date and time of the image
    satellite (str): either 'S2A' or 'S2B'
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

from tsd import search_scihub, utils

AWS_S3_URL_L1C = 's3://sentinel-s2-l1c'
AWS_S3_URL_L2A = 's3://sentinel-s2-l2a'
AWS_S3_URL_L2A_COGS = 'http://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs'
GCLOUD_URL = 'https://storage.googleapis.com/gcp-public-data-sentinel-2'
SCIHUB_API_URL = 'https://scihub.copernicus.eu/apihub/odata/v1'
RODA_URL = 'https://roda.sentinel-hub.com'

BANDS_L1C = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A',
             'B09', 'B10', 'B11', 'B12', 'TCI']

BANDS_L2A = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A',
             'B09', 'B11', 'B12', 'TCI', 'AOT', 'WVP', 'SCL']

BANDS_RESOLUTION = {'B01': 60,
                    'B02': 10,
                    'B03': 10,
                    'B04': 10,
                    'B05': 20,
                    'B06': 20,
                    'B07': 20,
                    'B08': 10,
                    'B8A': 20,
                    'B09': 60,
                    'B10': 60,
                    'B11': 20,
                    'B12': 20,
                    'TCI': 10,
                    'AOT': 60,
                    'WVP': 10,
                    'SCL': 20}

# Correspondence between band name and band index, from page 57 / 496 of
# https://sentinel.esa.int/documents/247904/349490/S2_MSI_Product_Specification.pdf
BANDS_INDEX = {'0': 'B01',
               '1': 'B02',
               '2': 'B03',
               '3': 'B04',
               '4': 'B05',
               '5': 'B06',
               '6': 'B07',
               '7': 'B08',
               '8': 'B8A',
               '9': 'B09',
               '10': 'B10',
               '11': 'B11',
               '12': 'B12'}


class ProductInfoNotFound(Exception):
    pass


class TileInfoNotFound(Exception):
    pass


def split_mgrs_id(mgrs_id):
    """
    Split an mgrs identifier such as 10SEG into (10, 'S', 'EG').
    """
    utm_zone, lat_band, sqid = re.split(r'(\d+)([a-zA-Z])([a-zA-Z]+)', mgrs_id)[1:4]
    utm_zone = int(utm_zone)
    return utm_zone, lat_band, sqid


def parse_safe_name_for_relative_orbit_number(safe_name):
    """
    Parse a SAFE name for the relative orbit number.

    Example of a SAFE name:
        S2A_MSIL1C_20180105T185751_N0206_R113_T10SEG_20180105T204427 --> 113
    """
    s = re.search('_R([0-9]{3})_', safe_name)
    return int(s.group(1))


def parse_safe_name_for_mgrs_id(safe_name):
    """
    Parse a SAFE name for its MGRS tile identifier.

    Example of a SAFE name:
        S2A_MSIL1C_20180105T185751_N0206_R113_T10SEG_20180105T204427 --> 10SEG
    """
    return re.findall(r"_T([0-9]{2}[A-Z]{3})_", safe_name)[0]


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
    Parse a SAFE name for the corresponding product type, either L1C or L2A.

    Example of a SAFE name:
        S2A_MSIL1C_20180105T185751_N0206_R113_T10SEG_20180105T204427 --> L1C
    """
    return re.findall(r"_MSIL(1C|2A)_", safe_name)[0]


def parse_datastrip_id_for_granule_date(datastrip_id):
    """
    Parse a datastrip id for the corresponding granule acquisition date.

    Examples of datastrip ids:
      S2B_OPER_MSI_L1C_DS_SGS__20180510T205109_S20180510T185438_N02.06 -> 20180510T185438
      S2A_OPER_MSI_L1C_DS_EPAE_20180516T000159_S20180515T190003_N02.06 -> 20180515T190003
    """
    date_str = re.findall(r"_S(2[0-9]{3}[0-1][0-9][0-3][0-9]T[0-9]{6})_",
                          datastrip_id)[0]
    return dateutil.parser.parse(date_str)


def parse_datatake_id_for_absolute_orbit(datatake_id):
    """
    Examples of datatake ids:
        GS2B_20180510T184929_006145_N02.06
        GS2A_20180515T184941_015125_N02.06
    """
    return int(datatake_id.split('_')[2])


def filename_from_metadata(img):
    """
    Args:
        img (Sentinel2Image instance): Sentinel-2 image metadata
    """
    return '{}_{}_orbit_{:03d}_tile_{}_L{}'.format(img.date.date().isoformat(),
                                                   img.satellite,
                                                   img.relative_orbit,
                                                   img.mgrs_id,
                                                   img.processing_level)


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
    granules = requests.get(granule_request,
                            auth=(search_scihub.read_copernicus_credentials_from_environment_variables())).json()
    return granules["d"]["results"][0]["Id"]


def get_s2_granule_id_of_scihub_item_from_sentinelhub(img):
    """
    Build the granule id of a given single tile SAFE.

    The hard part is to get the timestamp in the granule id. Unfortunately this
    timestamp is not part of the metadata returned by scihub. This function queries
    sentinelhub to retrieve it. It takes about 3 seconds.

    Args:
        img (Sentinel2Image instance): Sentinel-2 image metadata

    Return:
        str: granule id, e.g. L1C_T36RTV_A005095_20180226T084545
    """
    import sentinelhub
    t0 = (img.date - datetime.timedelta(hours=2)).isoformat()
    t1 = (img.date + datetime.timedelta(hours=2)).isoformat()
    r = sentinelhub.opensearch.get_tile_info('T{}'.format(img.mgrs_id), time=(t0, t1))
    assert(isinstance(r, dict))

    granule_date = dateutil.parser.parse(r['properties']['startDate']).strftime("%Y%m%dT%H%M%S")
    return "L1C_T{}_A{:06d}_{}".format(img.mgrs_id, img.relative_orbit, granule_date)


def get_roda_metadata(img, filename='tileInfo.json'):
    """
    Args:
        img (Sentinel2Image instance): Sentinel-2 image metadata

    Return:
        dict: content of the roda metadata json file
    """
    level = img.processing_level.lower()
    url = '{}/sentinel-s2-l{}/tiles/{}/{}/{}/{}/{}/{}/0/{}'.format(RODA_URL,
                                                                   level,
                                                                   img.utm_zone,
                                                                   img.lat_band,
                                                                   img.sqid,
                                                                   img.date.year,
                                                                   img.date.month,
                                                                   img.date.day,
                                                                   filename)
    r = requests.get(url)
    if r.ok:
        try:
            return json.loads(r.text)
        except json.decoder.JSONDecodeError:
            return r.text
    else:
        raise TileInfoNotFound("{} not found".format(url))


def get_roda_product_info(title):
    """
    Args:
        title (str): Sentinel-2 product title

    Return:
        dict: content of the roda productInfo.json metadata file
    """
    level = parse_safe_name_for_product_type(title).lower()
    date = parse_safe_name_for_acquisition_date(title)
    url = '{}/sentinel-s2-l{}/products/{}/{}/{}/{}/productInfo.json'.format(RODA_URL,
                                                                            level,
                                                                            date.year,
                                                                            date.month,
                                                                            date.day,
                                                                            title)
    r = requests.get(url)
    if r.ok:
        try:
            return json.loads(r.text)
        except json.decoder.JSONDecodeError:
            return r.text
    else:
        raise ProductInfoNotFound("{} not found".format(url))


class Sentinel2Image(dict):
    """
    Sentinel-2 image metadata class.
    """
    # use dict setters and getters, so that object interaction is like a dict
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, img, api='stac'):
        """
        """
        self.metadata_source = api

        if api == 'stac':
            self.stac_parser(img)
        elif api == 'scihub':
            self.scihub_parser(img)
        elif api == 'planet':
            self.planet_parser(img)
        elif api == 'gcloud':
            self.gcloud_parser(img)

        self.mgrs_id = parse_safe_name_for_mgrs_id(self.title)
        self.utm_zone, self.lat_band, self.sqid = split_mgrs_id(self.mgrs_id)
        self.epsg = utils.utm_to_epsg_code(self.utm_zone, self.lat_band)

        self.date = parse_safe_name_for_acquisition_date(self.title)
        self.relative_orbit = parse_safe_name_for_relative_orbit_number(self.title)
        self.processing_level = parse_safe_name_for_product_type(self.title)
        self.satellite = self.title[:3]  # S2A_MSIL1C_2018010... --> S2A

        self.filename = filename_from_metadata(self)
        self.urls = {'aws': {}, 'gcloud': {}}


    def stac_parser(self, img):
        """
        Args:
            img (dict): json metadata dict as shipped in stac API response
        """
        self.title = img['properties']['sentinel:product_id']
        self.geometry = img['geometry']
        self.cloud_cover = img['properties']['eo:cloud_cover']

        self.thumbnail = img['assets']['thumbnail']['href'] #.replace('sentinel-s2-l1c.s3.amazonaws.com',
                                                            #         'roda.sentinel-hub.com/sentinel-s2-l1c')
        self.aws_sequence_number = img["properties"]["sentinel:sequence"]
        self.aws_id = img["id"]
        self.source = [x["href"] for x in img["links"] if x["rel"] == "self"][0]


    def scihub_parser(self, img):
        """
        Args:
            img (dict): json metadata dict for a single SAFE, as shipped in scihub
                opensearch API response
        """
        self.title = img['title']
        self.absolute_orbit = img['orbitnumber']
        self.datatake_id = img['s2datatakeid']
        self.thumbnail = img['links']['icon']
        try:
            self.datastrip_id = img["datastripidentifier"]
            self.granule_date = parse_datastrip_id_for_granule_date(self.datastrip_id)
        except KeyError:
            pass

        s = img["footprint"]
        self.geometry = geojson.Feature(geometry=shapely.wkt.loads(s))["geometry"]


    def planet_parser(self, img):
        """
        Args:
            img (dict): json metadata dict for a single SAFE, as shipped in Planet
                API response
        """
        self.title = img['id']

        p = img['properties']
        self.absolute_orbit = p['abs_orbit_number']
        self.datatake_id = p["datatake_id"]
        self.granule_date = dateutil.parser.parse(p['acquired'])
        self.thumbnail = img['_links']['thumbnail']

        self.cloud_cover = p['cloud_cover']
        self.sun_azimuth = p['sun_azimuth']
        self.sun_elevation = p['sun_elevation']


    def gcloud_parser(self, img):
        """
        Args:
            img (dict): json metadata dict for a single SAFE, as shipped in Gcloud
                API response
        """
        self.title = img['product_id']

        self.absolute_orbit = int(img['granule_id'].split('_')[2][1:])
        self.granule_date = dateutil.parser.parse(img['sensing_time'], ignoretz=True)

        self.cloud_cover = img['cloud_cover']


    def build_gs_links(self):
        """
        Build Gcloud urls for all raster bands and the gml cloud mask.

        Example of url:
        https://storage.googleapis.com/gcp-public-data-sentinel-2/tiles/36/R/TV/S2B_MSIL1C_20180226T083909_N0206_R064_T36RTV_20180226T122942.SAFE/GRANULE/L1C_T36RTV_A005095_20180226T084545/IMG_DATA/T36RTV_20180226T083909_B01.jp2

        The tricky part is to build the granule name
        (L1C_T36RTV_A005095_20180226T084545 in the example above), which is not
        part neither of the stac nor of the scihub API responses. This function
        queries roda to retrieve it. It takes about 200 ms.
        """
        if 'granule_date' not in self:
            try:
                tile_info = get_roda_metadata(self, filename='tileInfo.json')
            except TileInfoNotFound:  # abort if file not found on roda
                return
            self.granule_date = parse_datastrip_id_for_granule_date(tile_info['datastrip']['id'])

        if 'absolute_orbit' not in self:
            try:
                product_info = get_roda_product_info(self.title)
            except ProductInfoNotFound:  # abort if file not found on roda
                return
            self.absolute_orbit = parse_datatake_id_for_absolute_orbit(product_info['datatakeIdentifier'])

        granule_id = 'L{}_T{}_A{:06d}_{}'.format(self.processing_level,
                                                 self.mgrs_id,
                                                 self.absolute_orbit,
                                                 self.granule_date.strftime("%Y%m%dT%H%M%S"))
        base_url = '{}/L2'.format(GCLOUD_URL) if self.processing_level == '2A' else GCLOUD_URL
        base_url += '/tiles/{}/{}/{}/{}.SAFE/GRANULE/{}'.format(self.utm_zone,
                                                                self.lat_band,
                                                                self.sqid,
                                                                self.title,
                                                                granule_id)
        urls = self.urls['gcloud']
        urls['cloud_mask'] = '{}/QI_DATA/MSK_CLOUDS_B00.gml'.format(base_url)

        if self.processing_level == '1C':
            for b in BANDS_L1C:
                urls[b] = '{}/IMG_DATA/T{}_{}_{}.jp2'.format(base_url,
                                                             self.mgrs_id,
                                                             self.date.strftime("%Y%m%dT%H%M%S"),
                                                             b)
        elif self.processing_level == '2A':
            for b in BANDS_L2A:
                urls[b] = '{}/IMG_DATA/R{}m/T{}_{}_{}_{}m.jp2'.format(base_url,
                                                                      BANDS_RESOLUTION[b],
                                                                      self.mgrs_id,
                                                                      self.date.strftime("%Y%m%dT%H%M%S"),
                                                                      b,
                                                                      BANDS_RESOLUTION[b])
        else:
            raise TypeError("processing_level of {} is neither L1C nor L2A".format(self['title']))


    def build_s3_links(self):
        """
        Build s3 urls for all raster bands and the gml cloud mask (if L1C).

        Examples of urls:
            L1C: s3://sentinel-s2-l1c/tiles/10/S/EG/2018/2/24/0/B04.jp2
            L2A: https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/10/S/EG/2018/2/S2B_10SEG_20180219_0_L2A/B04.tif
        """
        urls = self.urls['aws']

        if "aws_sequence_number" in self:  # this is the case when api == "stac"
            seq = self.aws_sequence_number
        else:
            try:
                product_info = get_roda_product_info(self.title)
                path = product_info["tiles"][0]["path"]
                seq = path.split("/")[-1]
            except ProductInfoNotFound:  # make an educated guess, assuming
                                         # that the sequence number is 0
                seq = "0"

        if self.processing_level == "1C":
            base_url = "{}/tiles/{}/{}/{}/{}/{}/{}/{}".format(AWS_S3_URL_L1C,
                                                              self.utm_zone,
                                                              self.lat_band,
                                                              self.sqid,
                                                              self.date.year,
                                                              self.date.month,
                                                              self.date.day,
                                                              seq)
            urls["cloud_mask"] = "{}/qi/MSK_CLOUDS_B00.gml".format(base_url)
            ext = "jp2"
            bands = BANDS_L1C

        elif self.processing_level == "2A":
            if "aws_id" in self:
                aws_id = self.aws_id
            else:
                aws_id = "{}_{}_{}_{}_L2A".format(self.satellite,
                                                  self.mgrs_id,
                                                  self.date.date().strftime("%Y%m%d"),
                                                  seq)

            base_url = "{}/{}/{}/{}/{}/{}/{}".format(AWS_S3_URL_L2A_COGS,
                                                     self.utm_zone,
                                                     self.lat_band,
                                                     self.sqid,
                                                     self.date.year,
                                                     self.date.month,
                                                     aws_id)
            urls["SCL"] = "{}/SCL.tif".format(base_url)
            ext = "tif"
            bands = BANDS_L2A

        for b in bands:
            urls[b] = "{}/{}.{}".format(base_url, b, ext)
#            if self.processing_level == "2A":
#                urls[b] = '{}/R{}m/{}.jp2'.format(base_url, BANDS_RESOLUTION[b], b)


    def get_satellite_angles(self):
        """
        Get satellite mean zenith and azimuth angles from xml metadata file.
        """
        try:
            metadata_xml = get_roda_metadata(self, filename="metadata.xml")
        except TileInfoNotFound:
            return

        d = xmltodict.parse(metadata_xml)

        angles = d["n1:Level-{}_Tile_ID".format(self.processing_level)]["n1:Geometric_Info"]["Tile_Angles"]["Mean_Viewing_Incidence_Angle_List"]["Mean_Viewing_Incidence_Angle"]
        self.satellite_zenith = dict(sorted([(BANDS_INDEX[x["@bandId"]], float(x["ZENITH_ANGLE"]["#text"])) for x in angles]))
        self.satellite_azimuth = dict(sorted([(BANDS_INDEX[x["@bandId"]], float(x["AZIMUTH_ANGLE"]["#text"])) for x in angles]))
