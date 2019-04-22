import dateutil

AWS_HTTPS_URL_L8 = 'https://landsat-pds.s3.amazonaws.com'
GCLOUD_URL_LANDSAT = 'https://storage.googleapis.com/gcp-public-data-landsat'
ALL_BANDS_LANDSAT = ['B{}'.format(i) for i in range(1, 12)] + ['BQA']


class LandsatGcloudParser:
    def __init__(self, img):
        self.meta = img
        self.urls = {'aws': {}, 'gcloud': {}}
        self._parse()
        self._build_gs_links()
        self.filename = '{}_{}_{}_{}'.format(self.date.date().isoformat(),
                                             self.satellite,
                                             self.sensor,
                                             self.scene_id)

    def _parse(self):
        d = self.meta.copy()
        self.scene_id = d['scene_id']
        self.satellite = d['spacecraft_id'].replace('ANDSAT_','')
        self.sensor = d['sensor_id'].replace('_', '')
        self.date = dateutil.parser.parse(d['sensing_time'], ignoretz=True)
        self.row = d['wrs_row']
        self.path = d['wrs_path']

    def _build_gs_links(self):
        base_url = self.meta['base_url']
        scene_id_bis = base_url.strip('/').split('/')[-1]
        for band in ALL_BANDS_LANDSAT:
            self.urls['gcloud'][band] = '{}/{}_{}.TIF'.format(base_url, scene_id_bis, band)


class LandsatDevSeedParser:
    def __init__(self, img):
        #self.meta = img
        p = img['properties']
        self.scene_id = p['landsat:scene_id']
        self.product_id = p['landsat:product_id']
        self.satellite = p['eo:platform'].replace('andsat-','').upper()
        self.sensor = p['eo:instrument'].replace('_', '')
        self.date = dateutil.parser.parse(p['datetime'], ignoretz=True)
        self.row = p['eo:row']
        self.path = p['eo:column']
        self.thumbnail = img['assets']['thumbnail']['href']
        self.cloud_cover = p['eo:cloud_cover']

        self.filename = '{}_{}_{}_{}'.format(self.date.date().isoformat(),
                                             self.satellite,
                                             self.sensor,
                                             self.scene_id)
        self.urls = {'aws': {}, 'gcloud': {}}

        if self.satellite == 'L8':
            for band in ALL_BANDS_LANDSAT:
                band_dico = img['assets'].get(band)
                if band_dico is not None:
                    self.urls['aws'][band] = band_dico['href']

        self._build_gs_links()

    def _build_gs_links(self):
        for band in ALL_BANDS_LANDSAT:
            sat_bis,*_,collec,_ = self.product_id.split('_')
            base_url = '{}/{}/{}/{}/{}/{}'.format(GCLOUD_URL_LANDSAT,sat_bis,collec,self.path,self.row, self.product_id)
            self.urls['gcloud'][band] = '{}/{}_{}.TIF'.format(base_url, self.product_id, band)
