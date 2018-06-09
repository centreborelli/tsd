"""
RPC model parsers, localization, and projection 
Copyright (C) 2015, Carlo de Franchis <carlo.de-franchis@cmla.ens-cachan.fr>
Copyright (C) 2015, Gabriele Facciolo <facciolo@cmla.ens-cachan.fr>
Copyright (C) 2015, Enric Meinhardt <enric.meinhardt@cmla.ens-cachan.fr>
"""


from __future__ import print_function
import copy
import numpy as np
from xml.etree.ElementTree import ElementTree


def apply_poly(poly, x, y, z):
    """
    Evaluates a 3-variables polynom of degree 3 on a triplet of numbers.

    Args:
        poly: list of the 20 coefficients of the 3-variate degree 3 polynom,
            ordered following the RPC convention.
        x, y, z: triplet of floats. They may be numpy arrays of same length.

    Returns:
        the value(s) of the polynom on the input point(s).
    """
    out = 0
    out += poly[0]
    out += poly[1]*y + poly[2]*x + poly[3]*z
    out += poly[4]*y*x + poly[5]*y*z +poly[6]*x*z
    out += poly[7]*y*y + poly[8]*x*x + poly[9]*z*z
    out += poly[10]*x*y*z
    out += poly[11]*y*y*y
    out += poly[12]*y*x*x + poly[13]*y*z*z + poly[14]*y*y*x
    out += poly[15]*x*x*x
    out += poly[16]*x*z*z + poly[17]*y*y*z + poly[18]*x*x*z
    out += poly[19]*z*z*z
    return out


def apply_rfm(num, den, x, y, z):
    """
    Evaluates a Rational Function Model (rfm), on a triplet of numbers.

    Args:
        num: list of the 20 coefficients of the numerator
        den: list of the 20 coefficients of the denominator
            All these coefficients are ordered following the RPC convention.
        x, y, z: triplet of floats. They may be numpy arrays of same length.

    Returns:
        the value(s) of the rfm on the input point(s).
    """
    return apply_poly(num, x, y, z) / apply_poly(den, x, y, z)


class RPCModel:
    def __init__(self, rpc_file):
        self.nan_rpc()
        self.read_rpc(rpc_file)

    def nan_rpc(self):
        self.row_offset = np.nan
        self.col_offset = np.nan
        self.lat_offset = np.nan
        self.lon_offset = np.nan
        self.alt_offset = np.nan
        self.row_scale = np.nan
        self.col_scale = np.nan
        self.lat_scale = np.nan
        self.lon_scale = np.nan
        self.alt_scale = np.nan
        self.lon_num = [np.nan] * 20
        self.lon_den = [np.nan] * 20
        self.lat_num = [np.nan] * 20
        self.lat_den = [np.nan] * 20
        self.row_num = [np.nan] * 20
        self.row_den = [np.nan] * 20
        self.col_num = [np.nan] * 20
        self.col_den = [np.nan] * 20

    def read_rpc(self, rpc_file):
        self.filepath = rpc_file

        if rpc_file.lower().endswith('xml'):
            tree = ElementTree()
            tree.parse(rpc_file)
            self.tree = tree   # store the xml tree in the object
            self.read_rpc_xml(tree)
        else:
            # we assume that non xml rpc files follow the ikonos convention
            self.read_rpc_ikonos(rpc_file)

    def read_rpc_ikonos(self, rpc_file):
        lines = open(rpc_file).read().split('\n')
        for l in lines:
            ll = l.split()
            if len(ll) > 1: self.add_tag_rpc(ll[0], ll[1])

    def add_tag_rpc(self, tag, val):
        a = tag.split('_')
        if len(a) == 2:
            if a[1] == "OFF:":
                if   a[0] == "LINE":   self.row_offset = float(val)
                elif a[0] == "SAMP":   self.col_offset = float(val)
                elif a[0] == "LAT":    self.lat_offset = float(val)
                elif a[0] == "LONG":   self.lon_offset = float(val)
                elif a[0] == "HEIGHT": self.alt_offset = float(val)
            elif a[1] == "SCALE:":
                if   a[0] == "LINE":   self.row_scale = float(val)
                elif a[0] == "SAMP":   self.col_scale = float(val)
                elif a[0] == "LAT":    self.lat_scale = float(val)
                elif a[0] == "LONG":   self.lon_scale = float(val)
                elif a[0] == "HEIGHT": self.alt_scale = float(val)

        elif len(a) == 4 and a[2] == "COEFF":
            # remove ':', convert to int and decrease the coeff index
            a[3] = int(a[3][:-1]) - 1
            if a[0] == "LINE":
                if   a[1] == "NUM": self.row_num[a[3]] = float(val)
                elif a[1] == "DEN": self.row_den[a[3]] = float(val)
            elif a[0] == "SAMP":
                if   a[1] == "NUM": self.col_num[a[3]] = float(val)
                elif a[1] == "DEN": self.col_den[a[3]] = float(val)

    def read_rpc_xml(self, tree):
        # determine wether it's a pleiades, spot-6 or worldview image
        a = tree.find('Metadata_Identification/METADATA_PROFILE') # PHR_SENSOR
        b = tree.find('IMD/IMAGE/SATID') # WorldView
        if a is not None:
            if a.text in ['PHR_SENSOR', 'S6_SENSOR', 'S7_SENSOR']:
                self.read_rpc_pleiades(tree)
            else:
                print('unknown sensor type')
        elif b is not None:
            if b.text == 'WV02' or b.text == 'WV01' or b.text == 'WV03':
                self.read_rpc_worldview(tree)
            else:
                print('unknown sensor type')


    def parse_coeff(self, element, prefix, indices):
        return [float(element.find("%s_%s" % (prefix, str(x))).text) for x in indices]


    def read_rpc_pleiades(self, tree):
        # localization function (from image to ground)
        d = tree.find('Rational_Function_Model/Global_RFM/Direct_Model')
        self.lon_num = self.parse_coeff(d, "SAMP_NUM_COEFF", range(1, 21))
        self.lon_den = self.parse_coeff(d, "SAMP_DEN_COEFF", range(1, 21))
        self.lat_num = self.parse_coeff(d, "LINE_NUM_COEFF", range(1, 21))
        self.lat_den = self.parse_coeff(d, "LINE_DEN_COEFF", range(1, 21))
        self.localization_bias = self.parse_coeff(d, "ERR_BIAS", ['X', 'Y'])
        
        # projection function (from ground to image)
        i = tree.find('Rational_Function_Model/Global_RFM/Inverse_Model')
        self.col_num = self.parse_coeff(i, "SAMP_NUM_COEFF", range(1, 21))
        self.col_den = self.parse_coeff(i, "SAMP_DEN_COEFF", range(1, 21))
        self.row_num = self.parse_coeff(i, "LINE_NUM_COEFF", range(1, 21))
        self.row_den = self.parse_coeff(i, "LINE_DEN_COEFF", range(1, 21))
        self.projection_bias = self.parse_coeff(i, "ERR_BIAS", ['ROW', 'COL'])
        
        # validity domains
        v = tree.find('Rational_Function_Model/Global_RFM/RFM_Validity')
        vd = v.find('Direct_Model_Validity_Domain')
        self.firstRow = float(vd.find('FIRST_ROW').text)
        self.firstCol = float(vd.find('FIRST_COL').text)
        self.last_row  = float(vd.find('LAST_ROW').text)
        self.last_col  = float(vd.find('LAST_COL').text)

        vi = v.find('Inverse_Model_Validity_Domain')
        self.firstLon = float(vi.find('FIRST_LON').text)
        self.firstLat = float(vi.find('FIRST_LAT').text)
        self.lastLon  = float(vi.find('LAST_LON').text)
        self.lastLat  = float(vi.find('LAST_LAT').text)

        # scale and offset
        # the -1 in line and column offsets is due to Pleiades RPC convention
        # that states that the top-left pixel of an image has coordinates
        # (1, 1)
        self.row_offset = float(v.find('LINE_OFF').text) - 1
        self.col_offset = float(v.find('SAMP_OFF').text) - 1
        self.lat_offset = float(v.find('LAT_OFF').text)
        self.lon_offset = float(v.find('LONG_OFF').text)
        self.alt_offset = float(v.find('HEIGHT_OFF').text)
        self.row_scale = float(v.find('LINE_SCALE').text)
        self.col_scale = float(v.find('SAMP_SCALE').text)
        self.lat_scale = float(v.find('LAT_SCALE').text)
        self.lon_scale = float(v.find('LONG_SCALE').text)
        self.alt_scale = float(v.find('HEIGHT_SCALE').text)

    def read_rpc_worldview(self, tree):
        # projection function
        im = tree.find('RPB/IMAGE')
        l = im.find('LINENUMCOEFList/LINENUMCOEF')
        self.row_num = [float(c) for c in l.text.split()]
        l = im.find('LINEDENCOEFList/LINEDENCOEF')
        self.row_den = [float(c) for c in l.text.split()]
        l = im.find('SAMPNUMCOEFList/SAMPNUMCOEF')
        self.col_num = [float(c) for c in l.text.split()]
        l = im.find('SAMPDENCOEFList/SAMPDENCOEF')
        self.col_den = [float(c) for c in l.text.split()]
        self.projection_bias = float(im.find('ERRBIAS').text)

        # scale and offset
        self.row_offset   = float(im.find('LINEOFFSET').text)
        self.col_offset   = float(im.find('SAMPOFFSET').text)
        self.lat_offset   = float(im.find('LATOFFSET').text)
        self.lon_offset   = float(im.find('LONGOFFSET').text)
        self.alt_offset   = float(im.find('HEIGHTOFFSET').text)

        self.row_scale = float(im.find('LINESCALE').text)
        self.col_scale = float(im.find('SAMPSCALE').text)
        self.lat_scale = float(im.find('LATSCALE').text)
        self.lon_scale = float(im.find('LONGSCALE').text)
        self.alt_scale = float(im.find('HEIGHTSCALE').text)

        # image dimensions
        self.last_row = int(tree.find('IMD/NUMROWS').text)
        self.last_col = int(tree.find('IMD/NUMCOLUMNS').text)


    def projection(self, lon, lat, alt):
        nlon = (lon - self.lon_offset) / self.lon_scale
        nlat = (lat - self.lat_offset) / self.lat_scale
        nalt = (alt - self.alt_offset) / self.alt_scale
        col = apply_rfm(self.col_num, self.col_den, nlat, nlon, nalt)
        row = apply_rfm(self.row_num, self.row_den, nlat, nlon, nalt)
        col = col * self.col_scale + self.col_offset
        row = row * self.row_scale + self.row_offset
        return col, row


    def localization(self, col, row, alt, return_normalized=False):

        if np.isnan(self.lat_num[0]):
            return self.localization_iterative(col, row, alt, return_normalized)

        ncol = (col - self.col_offset) / self.col_scale
        nrow = (row - self.row_offset) / self.row_scale
        nalt = (alt - self.alt_offset) / self.alt_scale
        lon = apply_rfm(self.lon_num, self.lon_den, nrow, ncol, nalt)
        lat = apply_rfm(self.lat_num, self.lat_den, nrow, ncol, nalt)
        if not return_normalized:
            lon = lon * self.lon_scale + self.lon_offset
            lat = lat * self.lat_scale + self.lat_offset
        return lon, lat


    def localization_iterative(self, col, row, alt, return_normalized=False):
        """
        Iterative estimation of the localization function (image to ground),
        for a list of image points expressed in image coordinates.

        Args:
            col, row: image coordinates
            alt: altitude (in meters above the ellipsoid) of the corresponding
                3D point
            return_normalized: boolean flag. If true, then return normalized
                coordinates

        Returns:
            lon, lat, alt
        """
        # normalise input image coordinates
        ncol = (col - self.col_offset) / self.col_scale
        nrow = (row - self.row_offset) / self.row_scale
        nalt = (alt - self.alt_offset) / self.alt_scale

        # target point: Xf (f for final)
        Xf = np.vstack([ncol, nrow]).T

        # use 3 corners of the lon, lat domain and project them into the image
        # to get the first estimation of (lon, lat)
        # EPS is 2 for the first iteration, then 0.1.
        lon = -np.ones(len(Xf))
        lat = -np.ones(len(Xf))
        EPS = 2
        x0 = apply_rfm(self.col_num, self.col_den, lat, lon, nalt)
        y0 = apply_rfm(self.row_num, self.row_den, lat, lon, nalt)
        x1 = apply_rfm(self.col_num, self.col_den, lat, lon + EPS, nalt)
        y1 = apply_rfm(self.row_num, self.row_den, lat, lon + EPS, nalt)
        x2 = apply_rfm(self.col_num, self.col_den, lat + EPS, lon, nalt)
        y2 = apply_rfm(self.row_num, self.row_den, lat + EPS, lon, nalt)

        n = 0
        while not np.all((x0 - ncol) ** 2 + (y0 - nrow) ** 2 < 1e-18):
            X0 = np.vstack([x0, y0]).T
            X1 = np.vstack([x1, y1]).T
            X2 = np.vstack([x2, y2]).T
            e1 = X1 - X0
            e2 = X2 - X0
            u  = Xf - X0

            # project u on the base (e1, e2): u = a1*e1 + a2*e2
            # the exact computation is given by:
            #   M = np.vstack((e1, e2)).T
            #   a = np.dot(np.linalg.inv(M), u)
            # but I don't know how to vectorize this.
            # Assuming that e1 and e2 are orthogonal, a1 is given by
            # <u, e1> / <e1, e1>
            num = np.sum(np.multiply(u, e1), axis=1)
            den = np.sum(np.multiply(e1, e1), axis=1)
            a1 = np.divide(num, den)

            num = np.sum(np.multiply(u, e2), axis=1)
            den = np.sum(np.multiply(e2, e2), axis=1)
            a2 = np.divide(num, den)

            # use the coefficients a1, a2 to compute an approximation of the
            # point on the gound which in turn will give us the new X0
            lon += a1 * EPS
            lat += a2 * EPS

            # update X0, X1 and X2
            EPS = .1
            x0 = apply_rfm(self.col_num, self.col_den, lat, lon, nalt)
            y0 = apply_rfm(self.row_num, self.row_den, lat, lon, nalt)
            x1 = apply_rfm(self.col_num, self.col_den, lat, lon + EPS, nalt)
            y1 = apply_rfm(self.row_num, self.row_den, lat, lon + EPS, nalt)
            x2 = apply_rfm(self.col_num, self.col_den, lat + EPS, lon, nalt)
            y2 = apply_rfm(self.row_num, self.row_den, lat + EPS, lon, nalt)
            #n += 1

        #print('localization_iterative: %d iterations' % n)

        if not return_normalized:
            lon = lon * self.lon_scale + self.lon_offset
            lat = lat * self.lat_scale + self.lat_offset

        if np.size(lon) == 1 and np.size(lat) == 1:
            return lon[0], lat[0]
        else:
            return lon, lat


    def __repr__(self):
        return """
    # Projection function coefficients
      col_num = {}
      col_den = {}
      row_num = {}
      row_den = {}

    # Offsets and Scales
      row_offset = {}
      col_offset = {}
      lat_offset = {}
      lon_offset = {}
      alt_offset = {}
      row_scale = {}
      col_scale = {}
      lat_scale = {}
      lon_scale = {}
      alt_scale = {}""".format(' '.join(['{: .4f}'.format(x) for x in self.col_num]),
                               ' '.join(['{: .4f}'.format(x) for x in self.col_den]),
                               ' '.join(['{: .4f}'.format(x) for x in self.row_num]),
                               ' '.join(['{: .4f}'.format(x) for x in self.row_den]),
                               self.row_offset,
                               self.col_offset,
                               self.lat_offset,
                               self.lon_offset,
                               self.alt_offset,
                               self.row_scale,
                               self.col_scale,
                               self.lat_scale,
                               self.lon_scale,
                               self.alt_scale)


if __name__ == '__main__':
    # test on the first haiti image
    rpc = RPCModel('pleiades_data/haiti/rpc01.xml')
    col, row = 20000, 8000
    alt = 90
    print('col={}, row={}, alt={}'.format(col, row, alt))
    lon, lat = rpc.localization(col, row, alt)
    print('lon={}, lat={}'.format(lon, lat))
    col, row = rpc.projection(lon, lat, alt)
    print('col={}, row={}'.format(col, row))
