"""
This module contains parsers for the Sentinel-1 metadata outputs of all the
search APIs supported by TSD, such as scihub and planet. Each
API parser receives as input a Python dict containing the metadata of an image
as returned by the API. It extracts from it the metadata that TSD needs and
stores them in an object with standard attributes (i.e. the attributes are the
same for all APIs). The detailed list of attributes is given below. This allows
TSD to use any search API with any download mirror.

Each parser returns a Sentinel1Image object with the following attributes:

    date (datetime.datetime): acquisition date and time of the image
    satellite (str): either 'S1A' or 'S1B'
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

import dateutil.parser
import requests
import xmltodict

from tsd import search_scihub, utils

AWS_S3_URL = 's3://sentinel-s1-l1c'
GCLOUD_URL = 'https://storage.googleapis.com/gcp-public-data-sentinel-1'  # TODO FIXME
SCIHUB_API_URL = 'https://scihub.copernicus.eu/apihub/odata/v1'
RODA_URL = 'https://roda.sentinel-hub.com/sentinel-s1-l1c/'


def parse_safe_name_for_relative_orbit_number(safe_name):
    """
    """
    s = re.search('_R([0-9]{3})_', safe_name)
    return int(s.group(1))


def parse_safe_name_for_acquisition_date(safe_name):
    """
    Parse a SAFE name for the corresponding acquisition date.

    Example of a SAFE name:
        S2A_MSIL1C_20180105T185751_N0206_R113_T10SEG_20180105T204427 --> 20180105T185751
    """
    date_str = re.findall(r"_(2[0-9]{3}[0-1][0-9][0-3][0-9]T[0-9]{6})_",
                          safe_name)[0]
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
        img (Sentinel1Image instance): Sentinel-1 image metadata
    """
    return '{}_{}_orbit_{:03d}_{}'.format(img.date.date().isoformat(),
                                          img.satellite,
                                          img.relative_orbit,
                                          img.product_type)


def get_roda_metadata(img, filename='tileInfo.json'):
    """
    Args:
        img (Sentinel1Image instance): Sentinel-1 image metadata

    Return:
        dict: content of the roda metadata json file
    """
    # https://roda.sentinel-hub.com/sentinel-s1-l1c/GRD/2019/12/21/IW/DV/S1B_IW_GRDH_1SDV_20191221T074101_20191221T074126_019460_024C2A_36DF/productInfo.json
    url = '{}/GRD/{}/{}/{}/{}/{}/0/{}'.format(RODA_URL, img.date.year,
                                              img.date.month, img.date.day,
                                              img.operational_mode,
                                              img.polarisation_string, img.safe,
                                              filename)
    r = requests.get(url)
    if r.ok:
        try:
            return json.loads(r.text)
        except json.decoder.JSONDecodeError:
            return r.text
    else:
        print("{} not found on roda".format(img.title, url))
        return None


def parse_polarisation_string(polarisation):
    """
    Convert polarisation string.

    Args:
        polarisation (str): polarisations list such as "VV VH", "HH HV", "VV"
            or "HH"

    Returns:
        two letters string (either "DV", "DH", "SV" or "SH")
    """
    if polarisation == "VV VH":
        return "DV"
    elif polarisation == "HH HV":
        return "DH"
    elif polarisation == "VV":
        return "SV"
    elif polarisation == "HH":
        return "SH"
    else:
        raise Exception("Unexpected polarisation string: {}".format(polarisation))


class Sentinel1Image(dict):
    """
    Sentinel-1 image metadata class.
    """
    # use dict setters and getters, so that object interaction is like a dict
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, img, api='devseed'):
        """
        """
        self.metadata_source = api
        #self.metadata_original = img

        if api == 'scihub':
            self.scihub_parser(img)
        elif api == 'planet':
            self.planet_parser(img)

        self.filename = filename_from_metadata(self)
        self.urls = {'aws': {}, 'gcloud': {}}


    def scihub_parser(self, img):
        """
        Args:
            img (dict): json metadata dict for a single SAFE, as shipped in scihub
                opensearch API response
        """
        self.title = img['title']
        self.date = dateutil.parser.parse(img['beginposition'], ignoretz=True)
        self.satellite = self.title[:3]  # S1A_IW_GRDH_1SDV_20191218... --> S1A
        self.absolute_orbit = img['orbitnumber']
        self.relative_orbit = img['relativeorbitnumber']
        self.operational_mode = img['sensoroperationalmode']
        self.polarisations = [p.lower() for p in img['polarisationmode'].split()]
        self.polarisation_string = parse_polarisation_string(img['polarisationmode'])
        self.product_type = img['producttype']
        self.footprint = img['footprint']
        self.thumbnail = img['links']['icon']


    def planet_parser(self, img):  #TODO FIXME
        """
        Args:
            img (dict): json metadata dict for a single SAFE, as shipped in Planet
                API response
        """
        self.title = img['id']
        p = img['properties']
        self.mgrs_id = p['mgrs_grid_id']
        self.utm_zone, self.lat_band, self.sqid = split_mgrs_id(self.mgrs_id)
        self.date = parse_safe_name_for_acquisition_date(self.title)  # 'acquired' contains the granule datetime
        self.satellite = p['satellite_id'].replace("Sentinel-", "S")  # Sentinel-2A --> S2A
        self.relative_orbit = p['rel_orbit_number']
        self.absolute_orbit = p['abs_orbit_number']
        self.granule_date = dateutil.parser.parse(p['acquired'])
        #self.granule_date = dateutil.parser.parse(p['granule_id'].split('_')[3])
        self.thumbnail = img['_links']['thumbnail']

        self.cloud_cover = p['cloud_cover']
        self.sun_azimuth = p['sun_azimuth']
        self.sun_elevation = p['sun_elevation']


    def build_gs_links(self):
        """
        Build Gcloud urls for the 13 jp2 bands and the gml cloud mask.

        Example of url:
        https://storage.googleapis.com/gcp-public-data-sentinel-2/tiles/36/R/TV/S2B_MSIL1C_20180226T083909_N0206_R064_T36RTV_20180226T122942.SAFE/GRANULE/L1C_T36RTV_A005095_20180226T084545/IMG_DATA/T36RTV_20180226T083909_B01.jp2

        The tricky part is to build the granule name
        (L1C_T36RTV_A005095_20180226T084545 in the example above), which is not
        part neither of the devseed nor of the scihub API responses. This function
        queries roda to retrieve it. It takes about 200 ms.
        """
        if 'granule_date' not in self:
            tile_info = get_roda_metadata(self, filename='tileInfo.json')
            #self.granule_date = dateutil.parser.parse(tile_info['timestamp'])
            if not tile_info:  # abort if file not found on roda
                return
            self.granule_date = parse_datastrip_id_for_granule_date(tile_info['datastrip']['id'])

        if 'absolute_orbit' not in self:
            product_info = get_roda_metadata(self, filename='productInfo.json')
            if not product_info:  # abort if file not found on roda
                return
            self.absolute_orbit = parse_datatake_id_for_absolute_orbit(product_info['datatakeIdentifier'])

    #    if self.is_old:
    #        img_name = '{}_{}.jp2'.format('_'.join(granule_id.split('_')[:-1]), '{}')
    #        cloud_mask_name = '{}_B00_MSIL1C.gml'.format('_'.join(granule_id.split('_')[:-1]).replace('MSI_L1C_TL', 'MSK_CLOUDS'))

        granule_id = 'L{}_T{}_A{:06d}_{}'.format(self.product_type,
                                                 self.mgrs_id,
                                                 self.absolute_orbit,
                                                 self.granule_date.strftime("%Y%m%dT%H%M%S"))
        base_url = '{}/L2'.format(GCLOUD_URL) if self.product_type == '2A' else GCLOUD_URL
        base_url += '/tiles/{}/{}/{}/{}.SAFE/GRANULE/{}'.format(self.utm_zone,
                                                                self.lat_band,
                                                                self.sqid,
                                                                self.title,
                                                                granule_id)
        urls = self.urls['gcloud']
        urls['cloud_mask'] = '{}/QI_DATA/MSK_CLOUDS_B00.gml'.format(base_url)
        for b in ALL_BANDS:
            if self.product_type == '1C':
                urls[b] = '{}/IMG_DATA/T{}_{}_{}.jp2'.format(base_url,
                                                             self.mgrs_id,
                                                             self.date.strftime("%Y%m%dT%H%M%S"),
                                                             b)
            elif self.product_type == '2A':
                urls[b] = '{}/IMG_DATA/R{}m/T{}_{}_{}_{}m.jp2'.format(base_url,
                                                                      BANDS_RESOLUTION[b],
                                                                      self.mgrs_id,
                                                                      self.date.strftime("%Y%m%dT%H%M%S"),
                                                                      b,
                                                                      BANDS_RESOLUTION[b])
            else:
                raise Exception("product_type of {} is neither L1C nor L2A".format(self['title']))


    def build_s3_links(self):
        """
        Build s3 urls for the tiff image files of a Sentinel-1 image.

        Example of url:
        s3://sentinel-s1-l1c/GRD/2019/12/21/IW/DV/S1B_IW_GRDH_1SDV_20191221T074101_20191221T074126_019460_024C2A_36DF/measurement/iw-vv.tiff
        """
        base_url = '{}/GRD/{}/{}/{}/{}/{}/{}/measurement'.format(AWS_S3_URL,
                                                                 self.date.year,
                                                                 self.date.month,
                                                                 self.date.day,
                                                                 self.operational_mode,
                                                                 self.polarisation_string,
                                                                 self.title)
        urls = self.urls['aws']
        for b in self.polarisations:
            urls[b] = '{}/iw-{}.tiff'.format(base_url, b)
