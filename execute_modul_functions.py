#--------------------------------------------------------------------------------------------------------------------------------
# Name:        execute_modul_functions
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
#--------------------------------------------------------------------------------------------------------------------------------

import modules.geo_position as geo
import numpy as np

from modules.handle_bbch_references import HandleBBCHReferences
from modules.interpolate_geotiffs import InterpolateGeotiffs
from rasdaman.credentials import Credentials
from rasdaman.datacube_S2 import DatacubeS2
from modules.rasdaman_request import RasdamanRequest
from modules.geojson_creator import GeoJsonCreator
from modules.gdal_tiff_functions import GdalTiffFunctions


def execute_BBCH_functions():
    input_folder = ""
    output_folder = ""
    HandleBBCHReferences.create_all_field_polygons(input_folder, output_folder)
    HandleBBCHReferences.calculate_median_bbch()


def execute_tiff_functions():

    exampleTiff = ""
    InterpolateGeotiffs.plot_geotiff(exampleTiff)


def access_dwd_coverages():
    path_to_geojson = ""
    poly = geo.transfer_geom(path_to_geojson, 25832, 25832)

    centroid = geo.get_centroid_bounds_area(poly)[0]
    easting = centroid.x
    northing = centroid.y
    print("E value: " + str(easting) + " N value: " + str(northing))

    # Example transformation
    point1 = geo.transfer_point((51.87175941405523,10.106714036711393), 4326, 25832)
    point2 = geo.transfer_point((52.75172612580286,14.379223135467132), 4326, 25832)

    # Example of working Rasdaman query
    #test = 'https://datacube.julius-kuehn.de/flf/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage&COVERAGEID' \
    #       '=DWD_Niederschlag&SUBSET=ansi("2020-06-01T00:00:00.000Z","2020-06-30T00:00:00.000Z")&SUBSET' \
    #       '=E(3370743,3370743)&SUBSET=N(5576237, 5576237)&FORMAT=text/csv'
    #data = RasdamanRequest.try_rastaman_request(test, user=Credentials.ras_user, passwd=Credentials.ras_pw)

    startdate_str = "2020-01-01"
    enddate_str = "2020-12-31"
    rainfall = RasdamanRequest.get_coverage_subset(startdate=startdate_str, enddate=enddate_str,
                                                     rasdaman_layer='DWD_Niederschlag',
                                                     easting=easting, northing=northing, user=Credentials.ras_user,
                                                     passwd=Credentials.ras_pw)

    temp_min = RasdamanRequest.get_coverage_subset(startdate=startdate_str, enddate=enddate_str,
                                                     rasdaman_layer='DWD_Temp_Min',
                                                     easting=easting, northing=northing, user=Credentials.ras_user,
                                                     passwd=Credentials.ras_pw)

    temp_max = RasdamanRequest.get_coverage_subset(startdate=startdate_str, enddate=enddate_str,
                                                     rasdaman_layer='DWD_Temp_Max',
                                                     easting=easting, northing=northing, user=Credentials.ras_user,
                                                     passwd=Credentials.ras_pw)

    temp_mean = RasdamanRequest.get_coverage_subset(startdate=startdate_str, enddate=enddate_str,
                                                      rasdaman_layer='DWD_Temp_Tagesmittel',
                                                      easting=easting, northing=northing, user=Credentials.ras_user,
                                                     passwd=Credentials.ras_pw)

    timeseries_rain = list(map(int, rainfall))
    timeseries_rain_arr = np.array(timeseries_rain)


def access_s2_datacube():
    """
    This function collects all available Sentinel-2 images for an agricultural field.
    The SAVI will be calculated in case the valid pixel portion is satisfied...
    """

    # choose a minimum portion of valid pixels within the image [in %]
    valid_pixels = 10

    # get phases from previous year
    year = '2019'
    days = RasdamanRequest.get_all_dates(year)

    # Geojson path in 4326
    geojson = ""
    poly = geo.transfer_geom(geojson, 4326, 25832)
    polygon = str(poly.wkt).replace(' (', '(')

    # Example polygon in 25832.
    # polygon = "POLYGON((608558 5787080, 616739 5787455, 617189 5780348, 609195 5783083, 608558 5787080))"

    for i, day in enumerate(days):

        # requesting DataCube
        img = DatacubeS2.get_S2_imagery(
            polygon=polygon,
            layer='S2_GermanyGrid',
            date="2018-06-02",
            user=Credentials.ras_cde_user,
            pw=Credentials.ras_cde_pw,
            host=Credentials.ras_cde_host,
            epsg=25832,
            band1='NIR10',
            band2='R',
            band3='G',
            band_subset=True,
            printout=False,
            get_query=False
        )

        path_to_tiff = ""
        RasdamanRequest().create_s2_tiff(img, path_to_tiff, valid_pixels)

        # Example to calculate SAVI
        index = RasdamanRequest.calculate_savi(img, valid_pixel_portion=valid_pixels)


def interpolate_bsc_in_field():

    path_to_tiff = ""
    output_folder = ""
    InterpolateGeotiffs.interpolate_tiff(path_to_tiff, output_folder)


def interpolate_s2_time_series():
    geojson = ""
    folder_to_series = ""
    output_folder = ""

    InterpolateGeotiffs.interpolate_tiffs(output_folder, folder_to_series, geojson, 0.5)


def translate_geotiffs_to_optimized():
    path_to_tiff_old = ""
    path_to_tiff_new = ""
    input_name = ""
    output_name = ""

    GdalTiffFunctions.cloud_optimize_gtiff(path_to_tiff_old, input_name, path_to_tiff_new, output_name)
    print("Success")


def main():

    # ----------------Here are examples of how diverse methods can be used------------ #

    execute_BBCH_functions()
    execute_tiff_functions()

    access_dwd_coverages()
    access_s2_datacube()
    interpolate_bsc_in_field()
    interpolate_s2_time_series()

    # Get the list of polygons for the field IDs
    field_ids = [352425739918, 82458386500, 248630816000, 329372329300, 62429230600, 7165676000, 223428868500, 135809748700, 16413293500, 329412970918, 284741186618, 333049510300, 179911767700, 359725137000, 374927737700, 179093811200]
    polygons = GeoJsonCreator.get_polygons_from_field_ids(field_ids, "field_c")

    file_name = ""

    # Create a GeoJSON containing a MultiPolygon and save it to a file
    GeoJsonCreator.create_multipolygon_geojson(polygons, file_name, "EPSG:4326")

    # Example usage:
    geojson_path = ""
    output_path = ""

    GeoJsonCreator.create_polygons_from_geojson(geojson_path, output_path, radius=5)

    translate_geotiffs_to_optimized()


if __name__ == "__main__":
    main()