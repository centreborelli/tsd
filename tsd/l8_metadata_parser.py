AWS_HTTPS_URL_L8 = 'https://landsat-pds.s3.amazonaws.com'
GCLOUD_URL_LANDSAT = 'https://storage.googleapis.com/gcp-public-data-landsat'
ALL_BANDS_LANDSAT = ['B{}'.format(i) for i in range(1,12)] + ['BQA']


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


