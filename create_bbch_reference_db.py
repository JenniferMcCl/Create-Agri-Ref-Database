#--------------------------------------------------------------------------------------------------------------------------------
# Name:        create_bbch_reference_db
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
#--------------------------------------------------------------------------------------------------------------------------------

from modules.handle_bbch_references import HandleBBCHReferences
from modules.access_sql import AccessSql
from modules.interpolate_geotiffs import InterpolateGeotiffs
from modules.file_utils import FileUtils
from modules.rasdaman_request import RasdamanRequest
from modules.date_transformer import DateTransformer
from modules.field_id_creator import FieldIdCreation
from rasdaman.credentials import Credentials

import modules.geo_position as geo
import os
import re
import geojson
import psycopg2
from datetime import datetime

# ------------------------These are helper methods for data access------------------------- #


def process_ww_regular_field_files(folder_path, db_connector):
    """
        Process GeoJSON files in the specified folder and insert data into the database.

        Parameters:
            - folder_path: Path to the folder containing GeoJSON files.
            - dictionary to derive the hashed field id
            - db_connector: Database connection object.
    """

    db_cursor = db_connector.cursor()

    # Go through all fields, get the dates, buffdistm from name and calculated field area and add to database
    for filename in os.listdir(folder_path):
        if filename.endswith('.geojson'):
            filepath = os.path.join(folder_path, filename)

            # Calculate area
            area = geo.calculate_area(filepath)
            hashed_field_id = FieldIdCreation.hash_from_geojson(filepath, "W-Weizen")

            with open(filepath, 'r', encoding='utf-8') as geojson_file:
                geojson_data = geojson.load(geojson_file)
                geometry = geojson_data.get("geometry")
                year = int(float(geojson_data.get("properties", {}).get("Year")))
                startdate = f"{str(year)}-01-01" if year else "0000-00-00"
                enddate = f"{str(year)}-12-31" if year else "0000-00-00"

                # Insert data into the database
                AccessSql.insert_field_row(db_connector, "field_regular_size", hashed_field_id, geometry, startdate,
                                       enddate, "W-Weizen", 0,
                                       area)


def process_field_files(folder_path, field_id_dict, db_connector):
    """
        Process GeoJSON files in the specified folder and insert data into the database.

        Parameters:
            - folder_path: Path to the folder containing GeoJSON files.
            - dictionary to derive the hashed field id
            - db_connector: Database connection object.
    """
    crop_type = "W-Weizen"
    buff_dist = 0

    # Go through all fields, get the dates, buffdistm from name and calculated field area and add to database
    for filename in os.listdir(folder_path):
        if filename.endswith('.geojson'):
            filepath = os.path.join(folder_path, filename)
            year = "0000"
            field_id = None

            # Extract information from the filename
            match = re.match(r"ZEPP_(\d+)_([A-Za-z-]+)_inBuf(\d+)m_(\d+)?\.geojson", filename)
            match2 = re.match(r"ZEPP_(\d+)_([A-Za-z-]+)_inBuf(\d+)m.geojson", filename)
            if match:
                field_id = match.group(1)
                crop_type = match.group(2)
                buff_distm = int(match.group(3))
                year = match.group(4)

            if match2:
                field_id = match2.group(1)
                crop_type = match2.group(2)
                buff_distm = int(match2.group(3))
                year = None

            if match or match2:
                startdate = f"{year}-01-01" if year else "0001-01-01"
                enddate = f"{year}-12-31" if year else "9999-12-31"

                # Calculate area
                area = geo.calculate_area(filepath)

                # Load GeoJSON geometry
                with open(filepath, 'r') as file:
                    geojson_data = geojson.load(file)

                if (field_id, "0000") in field_id_dict:
                    hashed_field_id = field_id_dict[(field_id, "0000")]
                elif (field_id, year) in field_id_dict:
                    hashed_field_id = field_id_dict[(field_id, year)]
                else:
                    print("Field id not in hashed dictionary.")
                    return

                # Insert data into the database
                AccessSql.insert_field_row(db_connector, "field_c", hashed_field_id, geojson_data, startdate, enddate, crop_type,
                                           buff_dist, area)


def add_fields_to_table(field_folder, field_id_dict):
    """This method adds entries to the field table of all geojsons in the given folder according to convention.

        Parameters:
            - field_folder: Path to the folder containing GeoJSON files.
            -  field_id_dict: Dictionary containing the hashed id values for the fields
    """
    print("Starting process.")
    db_connector, db_cursor = AccessSql.create_db_connection()

    if db_connector and db_cursor:
        # This is an alternative method to use here.
        # process_field_files(field_folder, field_id_dict, db_connector)
        process_ww_regular_field_files(field_folder, db_connector)
        db_connector.close()
        db_cursor.close()
    else:
        print("DB connection failed!!!")


def add_field_series_to_table(db_connector, start_date, end_date, field_id, id_date_bbch_dict, dwd_values, field_geojson,
                              bsc_series_folder,
                              coh_series_folder,
                              s2_series_folder,
                              s2_interp_folder,
                              field_id_dict):
    """
        Here all field series are added to the table containing, S1, S2, BBCH and DWD weather data.
        The data can be referenced by date, id and geojson polygon
    """

    # Create folder for specific field interpolated s2 data if not already created
    s2_interp_folder = s2_interp_folder
    if not os.path.exists(s2_interp_folder):
        os.mkdir(s2_interp_folder)

    # Get all possible dates
    dates = DateTransformer.generate_date_range(start_date, end_date)

    # Get list of bsc field timeseries folder names
    bsc_timeseries = os.listdir(bsc_series_folder)
    bsc_timeseries.sort()
    bsc_timeseries = FileUtils.remove_aux_xml(bsc_timeseries)

    # Get all dates where backscatter is available.
    available_bsc_dates = list(map(FileUtils.extract_date_from_tiff_path, bsc_timeseries))

    # Combine the folder path of each data item to each date
    bsc_timeseries_path = [bsc_series_folder + "/" + str(element) for element in bsc_timeseries]
    date_bsc_data_dict = dict(zip(available_bsc_dates, bsc_timeseries_path))

    # Get list of coherence field timeseries folder names
    coh_timeseries = os.listdir(coh_series_folder)
    coh_timeseries.sort()
    coh_timeseries = FileUtils.remove_aux_xml(coh_timeseries)

    available_coh_dates = list(map(FileUtils.extract_date_from_tiff_path, coh_timeseries))

    # Combine the folder path of each data item to each date
    coh_timeseries_path = [coh_series_folder + "/" + str(element) for element in coh_timeseries]
    date_coh_data_dict = dict(zip(available_coh_dates, coh_timeseries_path))

    # Get list of S2 field timeseries folder names
    s2_timeseries = os.listdir(s2_series_folder)
    s2_timeseries.sort()
    s2_timeseries = FileUtils.remove_aux_xml(s2_timeseries)

    available_s2_dates = list(map(FileUtils.extract_date_from_tiff_path, s2_timeseries))

    # Combine the folder path of each data item to each date
    s2_timeseries_path = [s2_series_folder + "/" + str(element) for element in s2_timeseries]
    date_s2_data_dict = dict(zip(available_s2_dates, s2_timeseries_path))

    # Make sure to extend this if more parameters are acquired and added to row.
    cur_bbch = cur_bsc = cur_coh = cur_s2 = cur_dwd = bsc_val = coh_val = s2_val = s2_interp = None

    if field_id in id_date_bbch_dict:
        id_date_bbch_dict_item = id_date_bbch_dict[int(field_id)]
    else:
        id_date_bbch_dict_item = None

    print("Current field id in process:" + field_id)

    # Iterate over all possible dates, require if the data is available, and enter available data to table.
    for i in range(len(dates)):
        cur_date_time = datetime.strptime(dates[i], "%Y-%m-%d").date()
        cur_date = dates[i]
        hashed_field_id = None

        if id_date_bbch_dict_item and cur_date in id_date_bbch_dict_item:
            cur_bbch = id_date_bbch_dict_item.get(cur_date)
        if cur_date in date_bsc_data_dict:
            cur_bsc = date_bsc_data_dict.get(cur_date)
        if cur_date in date_coh_data_dict:
            cur_coh = date_coh_data_dict.get(cur_date)
        if cur_date in date_s2_data_dict:
            cur_s2 = date_s2_data_dict.get(cur_date)
        if cur_date in dwd_values:
            cur_dwd = dwd_values.get(cur_date)

        if not cur_bsc and not cur_coh and not cur_s2:
            # Make sure to extend this if more parameters are acquired and added to row.
            cur_bbch = cur_bsc = cur_coh = cur_s2 = cur_dwd = bsc_val = coh_val = s2_val = s2_interp = None
            continue

        if cur_bsc:
            valid_pixels_bsc, total_pixels_bsc = InterpolateGeotiffs.valid_pixel_in_poly(field_geojson, cur_bsc)

            # According to amount of valid pixel in raster data, validity flag is set.
            print("Valid Pixels Bsc:", valid_pixels_bsc)
            print("Total Pixels Polygon with 2 bands:", total_pixels_bsc)
            amount = valid_pixels_bsc / total_pixels_bsc if total_pixels_bsc != 0 else 0
            bsc_val = amount > 0.5

        if cur_coh:
            valid_pixels_coh, total_pixels_coh = InterpolateGeotiffs.valid_pixel_in_poly(field_geojson,
                                                                                         cur_coh)
            # According to amount of valid pixel in raster data, validity flag is set.
            print("Valid Pixels Coh:", valid_pixels_coh)
            print("Total Pixels Polygon:", total_pixels_coh)
            amount = valid_pixels_coh / total_pixels_coh if total_pixels_coh != 0 else 0
            coh_val = amount > 0.5

        if cur_s2:
            # Takes into account, the no data value is 0 for S2 data
            valid_pixels_s2, total_pixels_s2 = InterpolateGeotiffs.valid_pixel_in_poly(field_geojson, cur_s2, 0)

            # According to amount of valid pixel in raster data, validity flag is set.
            print("Valid Pixels S2:", valid_pixels_s2)
            print("Total Pixels Polygon with 10 bands:", total_pixels_s2)
            amount = valid_pixels_s2 / total_pixels_s2 if total_pixels_s2 != 0 else 0
            s2_val = amount > 0.5

            # Here the S2 data is interpolated and added to the dedicated folder. This will only be performed once.
            s2_interp = None
            if s2_val:
                s2_interp = InterpolateGeotiffs.interpolate_tiff(cur_s2, s2_interp_folder)

        # Derive the id from the dictionary. This is NOT an efficient solution.
        year = cur_date[:4]
        if (field_id, "0000") in field_id_dict:
            hashed_field_id = field_id_dict[(field_id, "0000")]
        elif (field_id, year) in field_id_dict:
            hashed_field_id = field_id_dict[(field_id, year)]
        else:
            print("Something wrong with id" + str(field_id))

        db_cursor = db_connector.cursor()
        AccessSql.enter_partial_row(db_cursor, db_connector, "field_c", hashed_field_id, date=cur_date_time,
                                   bbch_phase=cur_bbch,
                                   bsc_data=cur_bsc, bsc_valid=bsc_val,
                                   coh_data=cur_coh, coh_valid=coh_val,
                                   s2_data=cur_s2, s2_valid=s2_val, s2_interp_data=s2_interp,
                                   temp_mean=int(cur_dwd[1]),
                                   precip=int(cur_dwd[0]))

        # Make sure to extend this if more parameters are acquired and added to row.
        cur_bbch = cur_bsc = cur_coh = cur_s2 = cur_dwd = bsc_val = coh_val = s2_val = s2_interp = None


# -------------------------These methods access data directly------------------------- #

def create_dwd_files():
    """
        This method creates dwd files for all field geojsons in a folder
        :return: No return value.
    """

    field_folder = "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_ZEPP_GJSONs/"
    dwd_series_folder = "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_dwd_cov_2017-2021/"

    # Access all field geojsons
    field_items = os.listdir(field_folder)
    field_items.sort()

    for k in range(0, len(field_items)):
        name_comps = field_items[k].replace(".geojson", "").split("_")
        field_id = name_comps[1]
        file_name = dwd_series_folder + "ZEPP_" + field_id + "_DWD_2017-2021.csv"
        start_date = "2018-01-01"
        end_date = "2021-12-31"

        # This is to handle special cases where field boundaries might have changed over the years.
        # Thus, a different geojson polygon must be used for each year. Here only 2017-2021 is taken
        if len(name_comps) == 5 and name_comps[4].isnumeric() and int(name_comps[4]) <= 2016:
            continue
        elif len(name_comps) == 5 and name_comps[4].isnumeric() and int(name_comps[4]) > 2016:
            file_name = dwd_series_folder + "ZEPP_" + field_id + "_DWD_" + name_comps[4] + ".csv"

            if os.path.exists(file_name):
                continue

            # Adjust date range to yearS
            start_date = start_date.replace("2017", name_comps[4])
            end_date = end_date.replace("2021", name_comps[4])

        elif len(name_comps) == 5:

            # This is for geojsons where the borders have been corrected.
            file_name = dwd_series_folder + "ZEPP_" + field_id + "_" + name_comps[4] + "_DWD_2017-2021.csv"

        elif os.path.exists(file_name):
            continue

        create_dwd_field_series(start_date, end_date, field_folder + field_items[k], file_name)

        # Reset
        start_date = "2018-01-01"
        end_date = "2021-12-31"


def create_dwd_field_series(start_date, end_date, field_geojson, csv_file_name):
    """
        This is a helper method to create a dwd csv file for a specific field.
        If the field is a multipolygon, the middle point in taken.
            :param start_date: The beginning date of the series to be created.
            :param end_date: The end date of the series to be created.
            :param field_geojson: The geojson of the field to derive weather data from
            :param csv_file_name: The name of the file to add the field series to.
            :return: No return value
    """
    poly = geo.transfer_geom(field_geojson, 25832, 31467)

    centroid = geo.get_centroid_bounds_area(poly)[0]
    easting = centroid.x
    northing = centroid.y
    print("E value: " + str(easting) + " N value: " + str(northing))

    dates = DateTransformer.generate_date_range(start_date, end_date)

    rainfall = RasdamanRequest.get_coverage_subset(startdate=start_date, enddate=end_date,
                                                   rasdaman_layer='DWD_Niederschlag',
                                                   easting=easting, northing=northing, user=Credentials.ras_user,
                                                   passwd=Credentials.ras_pw)

    temp_mean = RasdamanRequest.get_coverage_subset(startdate=start_date, enddate=end_date,
                                                    rasdaman_layer='DWD_Temp_Tagesmittel',
                                                    easting=easting, northing=northing, user=Credentials.ras_user,
                                                    passwd=Credentials.ras_pw)

    timeseries_rain = list(map(int, rainfall))
    timeseries_temp_mean = list(map(int, temp_mean))

    timeseries_dwd = FileUtils.create_date_value_pair_dict(dates, timeseries_rain, timeseries_temp_mean)

    FileUtils.write_dict_to_csv(timeseries_dwd, csv_file_name)


def add_field_series_table_entries(start_date, end_date, field_id_dict):
    """
        This creates all entries in the field_day table, where either S1 or S2 data is available.
        :param start_date: Start date to query by
        :param end_date: End date of query
        :param field_id_dict: Dictionary containing the hashed field id values.
        :return: No return value.
    """

    field_folder = ("/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_ZEPP_GJSONs/")
    bsc_folder = "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_bsc_field_series_2018-2021/"
    coh_folder = "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_coh_field_series_2018-2021/"
    s2_folder = "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_S2_field_series_2018-2021/"
    dwd_series_folder = "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_dwd_cov_2017-2021/"

    bbch_file = ("/media/data_storage_2/jennifer/development/test_output/reference_data/RLP_ZEPP_CSV/bbch_values_W-Raps2"
                 ".csv")

    s2_interp_folder = ("/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_S2_field_series_ip/")

    # Access all field geojsons and sort
    field_items = os.listdir(field_folder)
    field_items.sort()

    field_ids = FileUtils.read_column(bbch_file, 0, False)
    field_ids.pop(0)
    field_ids = [item for item in field_ids if (item != "")]

    bbch_orig = FileUtils.read_column(bbch_file, 2, True)
    bbch_orig.pop(0)

    date = FileUtils.read_column(bbch_file, 1, True)
    date.pop(0)

    date = DateTransformer.trans_compact_d_m_y_dates_to_sql_format(
        DateTransformer.trans_d_m_y_dates_to_compact_dates(date))

    date_bbch = list(zip(date, bbch_orig))
    date_bbch_groups = FileUtils.create_list_of_dicts(date_bbch, field_ids)
    field_ids = list(dict.fromkeys(field_ids))

    id_date_bbch_dict = dict(zip(field_ids, date_bbch_groups))

    db_connector, db_cursor = AccessSql.create_db_connection()

    if db_connector and db_cursor:

        for k in range(0, len(field_items)):
            name_comps = field_items[k].replace(".geojson", "").split("_")
            field_id = name_comps[1]
            file_name = dwd_series_folder + "ZEPP_" + field_id + "_DWD_2017-2021.csv"

            if len(name_comps) == 5 and name_comps[4].isnumeric() and int(name_comps[4]) <= 2016:
                continue
            elif len(name_comps) == 5 and name_comps[4].isnumeric() and int(name_comps[4]) > 2016:
                file_name = dwd_series_folder + "ZEPP_" + field_id + "_DWD_" + name_comps[4] + ".csv"

                # Adjust date range to required year.
                start_date = start_date.replace("2017", name_comps[4])
                end_date = end_date.replace("2021", name_comps[4])

            elif len(name_comps) == 5:
                file_name = dwd_series_folder + "ZEPP_" + field_id + "_" + name_comps[4] + "_DWD_2017-2021.csv"

            date_dwd = FileUtils.read_csv_to_dict(file_name)

            add_field_series_to_table(db_connector, start_date, end_date,
                                      field_id,
                                      id_date_bbch_dict, date_dwd,
                                      field_folder + field_items[k],
                                      bsc_folder + field_items[k].replace(".geojson", ""),
                                      coh_folder + field_items[k].replace(".geojson", ""),
                                      s2_folder + field_items[k].replace(".geojson", ""),
                                      s2_interp_folder,
                                      field_id_dict)

            # Reset adjusted date range
            start_date = "2017-01-01"
            end_date = "2021-12-31"

        db_connector.close()
        db_cursor.close()
    else:
        print("DB connection failed!!!")


def add_field_bbch_table_entries(folder_path, db_connector):
    db_cursor = db_connector.cursor()

    # Go through all fields
    for filename in os.listdir(folder_path):
        if filename.endswith('.geojson'):
            filepath = os.path.join(folder_path, filename)
        else:
            print("Something wrong with geojson file name." + filename)
            continue

        # Calculate area
        area = geo.calculate_area(filepath)

        with open(filepath, 'r', encoding='utf-8') as geojson_file:
            geojson_data = geojson.load(geojson_file)
            geometry = geojson_data.get("geometry")
            polygon = geometry['coordinates'][0]

            properties = geojson_data.get("properties", {})
            bbch_values = properties.get("BBCH")
            bbch_dates = properties.get("BDate")
            year = int(float(properties.get("Year")))
            seeding = properties.get("Seeding")

            startdate = f"{str(year)}-01-01" if year else "0000-00-00"
            enddate = f"{str(year)}-12-31" if year else "0000-00-00"
            crop_type = "W-Weizen"

            data = f'origin: "ZEPP", geom: {polygon}, startdate: {startdate}, enddate: {enddate}, crop_type: {crop_type}, buff_distm: {0}, size: {area}'
            hashed_field_id = FieldIdCreation.hash_data(data, "43218765", year)

            for i in range(0, len(bbch_values)):
                date = bbch_dates[i]
                bbch = bbch_values[i]

                AccessSql.enter_partial_row(db_cursor, db_connector, "field_day_regular_size", field_id=hashed_field_id, date=date, size=None, bbch_phase=bbch, bbch_sim=False,
                          bsc_data=None, bsc_interp_data=None, bsc_valid=None,
                          coh_data=None, coh_interp_data=None, coh_valid=None,
                          s2_data=None, s2_interp_data=None, s2_valid=None,
                          temp_min=None, temp_max=None, temp_mean=None, precip=None)


def create_tables_from_data():

    # Creation of the tables. Only has to be executed once.
    AccessSql.create_sql_database_and_tables("field_c", "field_day_c")

    # This loop only needs to be executed once. After that the csv files for DWD Coverage values can be accessed directly.
    #create_dwd_files()

    # Creation of hashed field ids of the .geojsons in the given folder.
    # Creation of a dictionary of hashed values paired with the original id and year.
    #field_folder = ("/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_ZEPP_GJSONs/")
    #field_id_dict = FieldIdCreation.create_id_dict(field_folder)

    field_folder = "/media/data_storage_2/jennifer/application_data/bbch_geojsons/"
    field_id_dict = None

    # This adds all fields to the "field" table according to convention.
    add_fields_to_table(field_folder, field_id_dict)

    # This creates all entries over the given dates and according to the hashed ids in the "field_day" table.
    # Currently, the S1 data is derived from preprocessed timeseries in folders. The S2 data is derived from Rasdaman
    # This will be updated to access the S1 Germany grid in Rasdaman as well.
    add_field_series_table_entries(start_date="2018-01-01", end_date="2021-12-31", field_id_dict=field_id_dict)

    # This is a convenience function to display the content of the table
    AccessSql.count_rows_in_table("public.field_day")

    # This is one of the filter functions returning all rows of the table according to the filter.
    # Main interface for ML applications
    rows = AccessSql.connect_and_filter_by_complete(True, True, True, True)

    # This is one of the filter functions quering the database by a given geojson polygon.
    geojson_poly = "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/field_series_tests/field_filter_test.geojson"
    AccessSql.query_by_geojson_polygon("field", "geom", geojson_poly, 25832)

    # Generate a list of file paths to plot multiple items in one plot.
    file_paths = [
            "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_bsc_field_series_2018-2021/"
            "ZEPP_17472362_W-Raps_inBuf5m_2016/"
            "20180103_S1A_VVVH_139_desc_BS_RLP_ZEPP_17472362_W-Raps_inBuf5m_2016.tif",
            "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_coh_field_series_2018-2021/"
            "ZEPP_17472362_W-Raps_inBuf5m_2016/"
            "20180104_20180110_S1B_VV_66_desc_coh6_RLP_ZEPP_17472362_W-Raps_inBuf5m_2016.tif",
            "/media/data_storage_2/jennifer/zepp_field_series_2017-2021/RLP_S2_field_series_2018-2021/"
            "ZEPP_17472362_W-Raps_inBuf5m_2016/20180110_S2_ZEPP_17472362_W-Raps_inBuf5m_2016.tif"
        ]
    InterpolateGeotiffs.plot_geotiffs(file_paths)

    print("Execution os complete.")

# ----------------Here are examples of how further module methods can be used------------ #

    inputFolder = "/media/data_storage_2/jennifer/development/test_output/reference_data/RLP_ZEPP_GJSON_WITH_BBCH/"
    outputFolder = "/media/data_storage_2/jennifer/development/test_output/reference_data/RLP_ZEPP_CSV/"

    HandleBBCHReferences().create_bbch_csv_file(inputFolder, outputFolder, "W-Raps")

    outputFolderJson = "/media/data_storage_2/jennifer/development/test_output/reference_data/RLP_ZEPP_GJSONs/"
#    HandleBBCHReferences().create_all_field_polygons(inputFolder, outputFolderJson)
#    HandleBBCHReferences.calculate_median_bbch()

    exampleTiff = ("/home/jennifer/data_storage_2/jennifer/development2/test_output/Bayern_mos_2017-2018/20170602_20170608_S1B_VV_66_desc_coh6_Bay.tif")
    InterpolateGeotiffs.plot_geotiff(exampleTiff)


def create_tables_from_unified_fields():
    # Creation of the tables. Only has to be executed once.
    AccessSql.create_sql_database_and_tables()

    field_folder = "/media/data_storage_2/jennifer/application_data/bbch_geojsons_bbox/"
    field_id_dict = None

    # This adds all fields to the "field" table according to convention.
    add_fields_to_table(field_folder, field_id_dict)

    db_connector, db_cursor = AccessSql.create_db_connection()
    add_field_bbch_table_entries(field_folder, db_connector)


def main():
    # Only has to be executed once
    create_tables_from_data()

    create_tables_from_unified_fields()


if __name__ == "__main__":
    main()
