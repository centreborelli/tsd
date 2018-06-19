#!/usr/bin/env python3
import sys
import re
import geojson


def main(kml_filename):
    """
    Extract information from the kml file distributed by ESA to describe the
    Sentinel-2 MGRS tiling grid.

    This file is distributed on ESA Sentinel website at:

    https://sentinel.esa.int/documents/247904/1955685/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml
    """
    float_pattern = '(-?[0-9]+\.?[0-9]*)'  # regex to match a float
    ll_pattern = '{0},{0},0'.format(float_pattern)  # regex to match lon,lat,0
    looking_for_mgrs_id = True

    with open(kml_filename, 'r') as f:
        for line in f:
            if looking_for_mgrs_id:
                mgrs_id = re.search('<name>([0-9]{2}[A-Z]{3})</name>', line)
                if mgrs_id:
                    print(mgrs_id.group(1), end=' ')
                    looking_for_mgrs_id = False
            else:
                ll_bbx = re.search(' '.join([ll_pattern]*5), line)
                if ll_bbx:
                    lons = list(map(float, ll_bbx.groups()[0::2]))
                    lats = list(map(float, ll_bbx.groups()[1::2]))
                    print(geojson.Polygon(list(zip(lons, lats))))
                    looking_for_mgrs_id = True


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("\t Usage: {} input.kml > output.txt".format(sys.argv[0]))
    else:
        main(sys.argv[1])
