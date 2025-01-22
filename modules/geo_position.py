# --------------------------------------------------------------------------------------------------------------------------------
# Name:        geo_position
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2023
# Copyright:   (c) jennifer.mcclelland 2022
#
# --------------------------------------------------------------------------------------------------------------------------------

import json
import geojson

from shapely.geometry import Point
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely import wkt, ops
from pyproj import Transformer

# Lots of helpers to translate coordinate types and systems.


def load_geojson(geo_json_file):
    with open(geo_json_file) as data:
        return json.load(data)


def load_wkt_from_geojson(geo_json_file):
    with open(geo_json_file) as data:
        geo = json.load(data)
    return geojson_to_wkt(geo)


def wkt_to_geojson(wkt_coords):
    g1 = wkt.loads(wkt_coords)
    g2 = geojson.Feature(geometry=g1, properties={})
    return g2.geometry


def geojson_to_wkt(geojson_coords):
    s = json.dumps(geojson_coords)
    g1 = geojson.loads(s)
    g2 = shape(g1)
    return g2.wkt


def snap_coords_to_wkt(snap_coords) -> str:
    transformationFunc = (lambda x: (' '.join((lambda y: [y[1], y[0]])(i.split(","))) for i in x))

    openPolygon = list(transformationFunc(snap_coords.split(" ")))
    openPolygon.append(openPolygon[0])

    coordinatesAsString = ','.join(openPolygon)
    return "POLYGON((" + coordinatesAsString + "))"


# This function takes a geojson and returns a crs transformed shape polygon
def transfer_geom(poly_json, old_crs, new_crs):

    # access geojson geometry as shape polygon
    with open(poly_json) as data:
        geoms = json.load(data)
        poly = shape(geoms)

        trans = Transformer.from_crs(old_crs, new_crs, always_xy=True)
        new_shape = ops.transform(trans.transform, poly)
        return new_shape


def transfer_point(point, old_crs, new_crs):
    point = Point(point[0], point[1])

    trans = Transformer.from_crs(old_crs, new_crs, always_xy=True)
    new_point = ops.transform(trans.transform, point)
    return new_point


def get_centroid_bounds_area(polygon):
    centroid = polygon.centroid
    area = polygon.area
    bounds = polygon.bounds

    return centroid, bounds, area


def calculate_area(geojson_path):
    """
    Calculate the area of a GeoJSON polygon in square meters.

    :param geojson_path: Path to the GeoJSON file.
    :return: Area in square meters.
    """

    # Load GeoJSON file
    with open(geojson_path, 'r') as file:
        geojson_data = geojson.load(file)

    if geojson_data.get("type") == "Feature":
        geojson_data = geojson_data.get("geometry")

    coords = geojson_data['coordinates']

    if isinstance(coords[0][0][0], list):
        multi_polygon = MultiPolygon([Polygon(coord[0]) for coord in coords])
        area = multi_polygon.area
    else:
        polygon = Polygon(coords[0])
        geom = shape(polygon)
        area = geom.area

    return area
