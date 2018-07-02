import os
import sys
import geojson
import kml2geojson


def keep_only_polygons_from_geometry_collection(g):
    """
    Remove all elements that aren't Polygons from the 'geometries' list.
    """
    g['geometries'] = [x for x in g['geometries'] if x['type'] == 'Polygon']


def remove_z_from_polygon_coordinates(p):
    """
    Remove the third (z) coordinate from the points of a polygon.
    """
    p['coordinates'] = [[x[:2] for x in p['coordinates'][0]]]


def main(kml_filename, verbose=False):
    """
    Extract information from the kml file distributed by ESA to describe the
    Sentinel-2 MGRS tiling grid.

    This file is distributed on ESA Sentinel website at:

    https://sentinel.esa.int/documents/247904/1955685/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml
    """
    kml2geojson.main.convert(kml_filename, 's2_mgrs_grid')
    with open(os.path.join('s2_mgrs_grid', kml_filename.replace('.kml', '.geojson')), 'r') as f:
        grid = geojson.load(f)

    mgrs_tiles = []
    for x in grid['features']:
        g = x['geometry']
        keep_only_polygons_from_geometry_collection(g)
        for p in g['geometries']:
            remove_z_from_polygon_coordinates(p)
        mgrs_id = x['properties']['name']
        mgrs_tiles.append(geojson.Feature(id=mgrs_id, geometry=g))
        if verbose:
            print(mgrs_id, end=' ')
            print(g)

    return geojson.FeatureCollection(mgrs_tiles)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("\t Usage: {} input.kml > output.geojson".format(sys.argv[0]))
    else:
        print(geojson.dumps(main(sys.argv[1]), indent=2))
