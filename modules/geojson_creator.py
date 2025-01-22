import json
from shapely.geometry import shape, MultiPolygon
from modules.access_sql import AccessSql
from pyproj import Transformer
import geojson
import os
import math
from shapely.geometry import Polygon


class GeoJsonCreator:
    """
    This class creates geojsons by combining polygons, replacing types or transforming coordinate systems.
    """

    @staticmethod
    def get_polygons_from_field_ids(field_ids, table_name):
        """
        Queries the AccessSql class for each field ID to retrieve polygons.
        Returns a list of polygons.
        """
        polygons = []
        for field_id in field_ids:
            polygon_geojson = AccessSql.get_polygon_by_field_id(field_id, table_name)
            if polygon_geojson:
                polygons.append(shape(json.loads(polygon_geojson)))  # Convert GeoJSON to Shapely geometry
        return polygons

    @staticmethod
    def create_multipolygon_geojson(polygons, output_path, src_crs):
        """
        Takes a list of polygons and creates a GeoJSON file containing a MultiPolygon.
        Saves it to the specified path.
        """
        if not polygons:
            print("No polygons to create a GeoJSON.")
            return

        transformed_polygons = [GeoJsonCreator.transform_geometry(polygon, src_crs) for polygon in polygons]

        # Create a MultiPolygon from the list of Shapely polygons
        multipolygon = MultiPolygon(transformed_polygons)

        # Create GeoJSON structure
        geojson_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": multipolygon.__geo_interface__,
                    "properties": {}
                }
            ]
        }

        # Save GeoJSON to file
        with open(output_path, 'w') as f:
            json.dump(geojson_data, f, indent=2)

        print(f"GeoJSON saved to {output_path}")

    @staticmethod
    def transform_geometry(polygon, src_crs):
        """
        Transform the polygon's coordinates from the source CRS to the target CRS.
        Params:
         - polygon: A Shapely polygon to transform.
         - src_crs: The source CRS in "EPSG" format (e.g., "EPSG:3857").
        Returns:
         - Transformed Shapely polygon in the target CRS.
        """
        # Initialize transformer for CRS conversion
        transformer = Transformer.from_crs("EPSG:25832", src_crs, always_xy=True)

        # Transform each coordinate of the polygon
        def transform_coords(coords):
            return [transformer.transform(x, y) for x, y in coords]

        # Apply transformation to all polygons' exterior and interior coordinates
        transformed_polygon = shape({
            'type': polygon.geom_type,
            'coordinates': [
                               transform_coords(polygon.exterior.coords)  # Exterior
                           ] + [
                               transform_coords(interior.coords) for interior in polygon.interiors  # Interiors
                           ]
        })

        return transformed_polygon

    @staticmethod
    def create_circle_around_point(point, radius, num_points=32):
        """Create a circular polygon around a point with a given radius (in meters)."""
        circle_points = []
        for i in range(num_points):
            angle = 2 * math.pi * (i / num_points)
            dx = radius * math.cos(angle)
            dy = radius * math.sin(angle)
            circle_points.append((point[0] + dx, point[1] + dy))
        return Polygon(circle_points)

    @staticmethod
    def create_polygons_from_geojson(input_geojson_path, output_folder, radius):
        """Create new GeoJSON files for each point in the input GeoJSON, with polygons around them."""
        # Load the input GeoJSON file
        with open(input_geojson_path, 'r') as f:
            geojson_data = geojson.load(f)

        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)

        # Iterate over features and create polygons
        for feature in geojson_data['features']:
            grid_id = feature['properties']['GRID_ID']
            point_coords = feature['geometry']['coordinates']

            # Create a circle (polygon) around the point
            #polygon_coords = GeoJsonCreator.create_circle_around_point(point_coords, radius)

            geojson_data = {
                        "type": "Feature",
                        "geometry": {"type": "Polygon",
                         "coordinates": point_coords},
                        "properties": {}
                        }

            # Save the new GeoJSON file
            output_path = os.path.join(output_folder, f"grid_id_{grid_id}.geojson")
            with open(output_path, 'w') as out_f:
                geojson.dump(geojson_data, out_f, indent=4)
            print(f"Created: {output_path}")


