"""
This module contains parsers for the Landsat metadata outputs of all the
search APIs supported by TSD, such as stac, planet, and gcloud. Each
API parser receives as input a Python dict containing the metadata of an image
as returned by the API. It extracts from it the metadata that TSD needs and
stores them in an object with standard attributes (i.e. the attributes are the
same for all APIs). The detailed list of attributes is given below. This allows
TSD to use any search API with any download mirror.

Each parser returns a LandsatImage object with the following attributes:

    row (int): Landsat Worldwide Reference System row
    path (int): Landsat Worldwide Reference System path
    date (datetime.datetime): acquisition date and time of the image
    satellite (str): either 'L8', 'L7', 'L5', 'L4' or 'L1'
    product_id (str): original name of the folder in which the image is packaged by NASA/USGS
    filename (str): string that TSD uses to name the crops downloaded for the bands
        of this image. It starts with the acquisition year, month and day so that
        sorting the files per image acquisition date is easy.
    urls (dict): dict with keys 'aws' and 'gcloud'. The value associated to
        each key is a dict with one key per band containing download urls.
    metadata_original (dict): the original response of the API for this image
"""
import dateutil

AWS_HTTPS_URL_L8 = 'https://s3-us-west-2.amazonaws.com/landsat-pds'
GCLOUD_URL = 'https://storage.googleapis.com/'
GCLOUD_BUCKET_LANDSAT = 'gcp-public-data-landsat'
ALL_BANDS_LANDSAT = ['B{}'.format(i) for i in range(1, 12)] + ['BQA']


def filename_from_metadata(img):
    """
    Args:
        img (LandsatImage instance): Landsat image metadata
    """
    return '{}_{}_{}_{}'.format(img.date.date().isoformat(), img.satellite,
                                img.sensor, img.scene_id)

class LandsatImage(dict):
    """
    Landsat image metadata class.
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
        elif api == 'planet':
            self.planet_parser(img)
        elif api == 'gcloud':
            self.gcloud_parser(img)

        self.filename = filename_from_metadata(self)

        self.urls = {'aws': {}, 'gcloud': {}}
        self.build_aws_links()
        self.build_gs_links()


    def stac_parser(self, img):
        """
        Args:
            img (dict): json metadata dict as shipped in stac API response
        """
        p = img['properties']
        self.scene_id = p['landsat:scene_id']
        self.satellite = p['platform'].replace('ANDSAT_', '')
        self.sensor = p['instruments'][0] + p['instruments'][1]
        self.date = dateutil.parser.parse(p['datetime'], ignoretz=True)
        self.row = int(p['landsat:wrs_row'])
        self.path = int(p['landsat:wrs_path'])

        self.product_id = img['id']
        self.thumbnail = img['assets']['thumbnail']['href']
        self.cloud_cover = p['eo:cloud_cover']
        self.aws_base_url = img['assets']['index']['href'].replace('/index.html', '')


    def gcloud_parser(self, d):
        """
        Args:
            d (dict): json metadata dict as shipped in gcloud API response
        """
        self.scene_id = d['scene_id']
        self.satellite = d['spacecraft_id'].replace('ANDSAT_','')
        self.sensor = d['sensor_id'].replace('_', '')
        self.date = dateutil.parser.parse(d['sensing_time'], ignoretz=True)
        self.row = d['wrs_row']
        self.path = d['wrs_path']

        self.product_id = d['product_id']
        #self.product_id = d['base_url'].strip('/').split('/')[-1]
        self.cloud_cover = d['cloud_cover']

        self.collection = d['collection_number']
        self.gcloud_base_url = d['base_url']


    def build_aws_links(self):
        """
        Build AWS urls for all the available Landsat bands.

        Exemple of base url:
            https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/044/034/LC08_L1TP_044034_20180225_20180308_01_T1'
        """
        if self.satellite != 'L8':
            return

        base_url = '{}/c1/L8/{:03d}/{:03d}/{}'.format(AWS_HTTPS_URL_L8,
                                                      self.path, self.row,
                                                      self.product_id)

        for band in ALL_BANDS_LANDSAT:
            self.urls['aws'][band] = '{}/{}_{}.TIF'.format(base_url,
                                                           self.product_id,
                                                           band)

    def build_gs_links(self):
        """
        Build Gcloud urls for all the available Landsat bands.
        """
        if 'gcloud_base_url' in self:
            base_url = self.gcloud_base_url.replace('gs://', GCLOUD_URL)
        else:
            sat, *_, collection, _ = self.product_id.split('_')
            base_url = '{}{}/{}/{}/{:03d}/{:03d}/{}'.format(GCLOUD_URL,
                                                            GCLOUD_BUCKET_LANDSAT,
                                                            sat, collection,
                                                            self.path,
                                                            self.row,
                                                            self.product_id)
        for band in ALL_BANDS_LANDSAT:
            self.urls['gcloud'][band] = '{}/{}_{}.TIF'.format(base_url,
                                                              self.product_id,
                                                              band)
