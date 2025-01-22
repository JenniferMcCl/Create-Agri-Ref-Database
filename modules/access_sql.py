#--------------------------------------------------------------------------------------------------------------------------------
# Name:        access_sql
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
#--------------------------------------------------------------------------------------------------------------------------------

import time
import geojson
import numpy as np

from typing import List, Tuple, Any, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

from rasterio.io import MemoryFile

from modules.interpolate_geotiffs import InterpolateGeotiffs
import modules.geo_position as geo


class AccessSql:
    """
    This is the main class to create and access the agriRef PostGreSql database and tables.
    """

    # This holds the db cursor. Only needs to be opened once
    db_cursor = None

    # Configure logging can be activated when needed.
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def create_db_connection():
        """
        This creates the connection to the database.
        :return: db_connector, db_cursor
        """

        db_connector = None
        try:
            # Password must be set.
            db_connector = psycopg2.connect(database="agriRef", user="postgres", host="localhost",
                                            password="", port="5432")

            db_cursor = db_connector.cursor()

            # Very important command to active GDAL drivers.
            query = "SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL'"
            db_cursor.execute(query)

            # Only execute this to check if drivers are active.
            # AccessSql.print_gdal_drivers(db_connector)

            return db_connector, db_cursor

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return None, None

    @staticmethod
    def print_gdal_drivers(db_connection):
        """
        This prints the active drivers to be able to use Postgis gdal commands
        :param db_connection: The active connection.
        :return:
        """

        try:
            # Establish a database connection
            cursor = db_connection.cursor()

            # Execute the query to get GDAL drivers
            query = "SELECT * FROM ST_GDALDrivers();"
            cursor.execute(query)

            # Fetch all results
            drivers = cursor.fetchall()

            # Check if any drivers are returned
            if not drivers:
                print("No GDAL drivers found.")
                return

            # Print the GDAL drivers
            print("Valid GDAL Drivers:")
            for driver in drivers:
                print(driver[0])

            # Close the cursor
            cursor.close()

        except psycopg2.Error as e:
            print(f"Error executing query: {e}")

    @staticmethod
    def create_sql_database_and_tables(field_table_name, field_day_table_name):
        """
        This creates the 2 relevant tables for holding and accessing geo-referenced areas including relevant data and information
        via SQL commands. The main purpose is to hold, maintain and filter reference data for ML algorithms.
        :param field_table_name: The table holding all relevant areas as geojson polygons.
        :param field_day_table_name: The Table holding the data for each day and field available.
        :return:
        """
        db_connector = None
        try:
            db_connector, AccessSql.db_cursor = AccessSql.create_db_connection()

            AccessSql.db_cursor.execute("create extension if not exists postgis; create extension if not exists postgis_raster;")
            print(AccessSql.db_cursor.statusmessage)

            AccessSql.db_cursor.execute("create extension if not exists timescaledb cascade;")
            print(AccessSql.db_cursor.statusmessage)

            # Activating this will delete all table content.
#            db_cursor.execute("DROP TABLE public.field_c")
#            db_cursor.execute("DROP TABLE public.field_day_c")

            AccessSql.db_cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.{} (
                    field_id BIGINT,
                    geom geometry(Geometry, 25832),  -- Geometry column with SRID 25832
                    startdate DATE,
                    enddate DATE,
                    crop_type TEXT,
                    buff_distm INTEGER,
                    size BIGINT
                );
            """.format(field_table_name))

            print("Creating table field: " + AccessSql.db_cursor.statusmessage)

            AccessSql.db_cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.{} (
                    field_id BIGINT, 
                    date DATE,
                    size INTEGER,
                    bbch_phase INTEGER,
                    bbch_sim BOOLEAN,
                    bsc_data raster CONSTRAINT enforce_srid_bsc CHECK (ST_SRID(bsc_data) = 25832),
                    bsc_interp_data raster CONSTRAINT enforce_srid_bsc_interp CHECK (ST_SRID(bsc_interp_data) = 25832),                 
                    bsc_valid BOOLEAN,
                    coh_data raster CONSTRAINT enforce_srid_coh CHECK (ST_SRID(coh_data) = 25832),
                    coh_interp_data raster CONSTRAINT enforce_srid_coh_interp CHECK (ST_SRID(coh_interp_data) = 25832),                     
                    coh_valid BOOLEAN,
                    s2_data raster CONSTRAINT enforce_srid_s2 CHECK (ST_SRID(s2_data) = 25832), 
                    s2_interp_data raster CONSTRAINT enforce_srid_s2_interp CHECK (ST_SRID(s2_interp_data) = 25832), 
                    s2_valid BOOLEAN,
                    temp_min INTEGER,
                    temp_max INTEGER,
                    temp_mean INTEGER,
                    precip INTEGER
                );
            """.format(field_day_table_name))

            print("Creating table field_day: " + AccessSql.db_cursor.statusmessage)

            AccessSql.db_cursor.execute("SELECT create_hypertable('field_day', 'date', 'field_id', 2);")
            print("Creating hypertable: " + AccessSql.db_cursor.statusmessage)

            db_connector.commit()

            AccessSql.db_cursor.execute("select * from information_schema.tables;")

            print(AccessSql.db_cursor.fetchone() + ", size: " + AccessSql.db_cursor.arraysize)
            print(AccessSql.db_cursor.fetchmany() + ", fetch all: " + AccessSql.db_cursor.fetchall() +
                  ", amount rows: " + AccessSql.db_cursor.rowcount)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if db_connector is not None:
                db_connector.close()

    @staticmethod
    def count_rows_in_table(table_name):
        """
            Counts the amount of rows in give table and prints the result.

            Parameters:
                table_name (str): the name of the table
        """
        try:
            db_connector = psycopg2.connect(database="agriRef", user="postgres", host="localhost",
                                            password="sentinel22", port="5432")
            db_cursor = db_connector.cursor()

            # Execute the SQL query to count rows in the table
            query = f"SELECT COUNT(*) FROM {table_name};"
            db_cursor.execute(query)

            # Fetch the result
            row_count = db_cursor.fetchone()[0]

            # Print the result
            print(f"Number of rows in '{table_name}': {row_count}")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

        finally:
            # Close the cursor and connection
            if db_connector:
                db_cursor.close()
                db_connector.close()
                print("PostgreSQL connection is closed")

    # ------------------Methods that access field_c table-----------------
    @staticmethod
    def get_polygon_by_field_id(field_id, table_name):
        """
        Query the database to retrieve the geometry polygon for a specific field_id.
        """
        query = sql.SQL("""
               SELECT ST_AsGeoJSON(geom) as geometry 
               FROM {} 
               WHERE field_id = %s;
           """.format(table_name))

        db_connector, db_cursor = AccessSql.create_db_connection()

        with db_cursor as cursor:
            cursor.execute(query, (field_id,))
            result = cursor.fetchone()
            if result:
                return result[0]  # The geometry column in GeoJSON format
            else:
                return None

    # ------------------Methods that access field_day_c table------------
    @staticmethod
    def insert_field_row(db_connector, table_name, field_id, geom, startdate, enddate, crop_type, buff_distm, size):
        """
        Insert data into the PostgreSQL field table.

        Parameters:
        - db_connector: Database connection object.
        - field_id: Field ID.
        - geom: Geometry object.
        - startdate: Start date.
        - enddate: End date.
        - crop_type: Crop type.
        - buff_distm: Buffer distance in meters.
        - size: Size of the field.
        """
        cursor = db_connector.cursor()
        insert_query = sql.SQL("""
            INSERT INTO public.{} (field_id, geom, startdate, enddate, crop_type, buff_distm, size)
            VALUES (%s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 25832), %s, %s, %s, %s, %s)
        """.format(table_name))

        rounded_size = int(round(size, 0))

        print(f"field_id: {field_id}")
        print(f"geom: {geojson.dumps(geom)}")
        print(f"startdate: {startdate}")
        print(f"enddate: {enddate}")
        print(f"crop_type: {crop_type}")
        print(f"buff_distm: {buff_distm}")
        print(f"rounded_size: {rounded_size}")

        cursor.execute(insert_query, (field_id, geojson.dumps(geom), startdate, enddate, crop_type, buff_distm, rounded_size))
        db_connector.commit()
        cursor.close()

    @staticmethod
    def fetch_row_from_db(db_connector, table_name, field_id, date):
        """
        Fetch a row from the 'field_day' table in PostgreSQL by field_id and date.

        Parameters:
        - db_connector: psycopg2 database connection object
        - field_id: ID of the field
        - date: Date of the data

        Returns:
        - Tuple containing all column values of the fetched row
        """
        try:
            query = """
                 SELECT field_id,
                        date,
                        size,
                        bbch_phase,
                        bbch_sim,
                        ST_AsGDALRaster(bsc_data, 'GTIFF') AS bsc_data,
                        ST_AsGDALRaster(bsc_interp_data, 'GTIFF') AS bsc_interp_data,
                        bsc_valid,
                        ST_AsGDALRaster(coh_data, 'GTIFF') AS coh_data,
                        ST_AsGDALRaster(coh_interp_data, 'GTIFF') AS coh_interp_data,
                        coh_valid,
                        ST_AsGDALRaster(s2_data, 'GTIFF') AS s2_data,
                        ST_AsGDALRaster(s2_interp_data, 'GTIFF') AS s2_interp_data,
                        s2_valid,
                        temp_min,
                        temp_max,
                        temp_mean,
                        precip
                 FROM public.{}
                 WHERE field_id = %s AND date = %s
             """.format(table_name)

            with db_connector.cursor() as cursor:
                cursor.execute(query, (field_id, date))
                row = cursor.fetchone()

            return row

        except psycopg2.Error as e:
            print(f"Error fetching row from database: {e}")
            return None

    @staticmethod
    def connect_and_fetch_row(field_id, table_name, date):
        """
        This connects to the db and retrieves a row fromthe given table for the given field_id and date.
        :param field_id: The field_id. This is a hashed value created with sensitive information.
        :param table_name: The name of the table to retrieve the row from.
        :param date: The date to receive teh row from.
        :return:
        """

        db_connector = None
        row = None
        try:
            db_connector = psycopg2.connect(database="agriRef", user="postgres", host="localhost",
                                            password="sentinel22", port="5432")

            cursor = db_connector.cursor()

            # Very important command to active GDAL drivers.
            query = "SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL'"
            cursor.execute(query)

            # Only execute this to check if drivers are active.
            # AccessSql.print_gdal_drivers(db_connector)

            row = AccessSql.fetch_row_from_db(db_connector, table_name, field_id, date)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return None
        finally:
            if db_connector is not None:
                db_connector.close()
                return row

    @staticmethod
    def insert_complete_row(db_cursor, db_connector, table_name, field_id, date, size, bbch_phase, bbch_sim,
                            bsc_data, bsc_interp_data, bsc_valid,
                            coh_data, coh_interp_data, coh_valid,
                            s2_data, s2_interp_data, s2_valid,
                            temp_min, temp_max, temp_mean, precip):

        # TODO: Test this code
        """
        Insert a complete row into the field_day table. Seldom possible since data is rarely complete.

        Parameters:
            db_cursor (psycopg2.extensions.cursor): The database cursor object.
            db_connector (psycopg2.extensions.connection): The database connection object.
            table_name: The name of the table to insert to.
            field_id (int): The identifier of the field.
            date (str): The date of the observation.
            size (int): The size of the field.
            bbch_phase (int): The BBCH phase.
            bbch_sim (bool): Indicates whether the BBCH phase is simulated.
            bsc_data (str): The name of the BSC raster.
            bsc_interp_data (str): The name of the interpolated BSC raster.
            bsc_valid (bool): Indicates whether the BSC data is valid.
            coh_data (str): The name of the COH raster.
            coh_interp_data (str): The name of the interpolated COH raster.
            coh_valid (bool): Indicates whether the COH data is valid.
            s2_data (str): The name of the S2 raster.
            s2_interp_data (str): The name of the interpolated S2 raster.
            s2_valid (bool): Indicates whether the S2 data is valid.
            temp_min (float): The minimum temperature.
            temp_max (float): The maximum temperature.
            temp_mean (float): The mean temperature.
            precip (float): The precipitation.
        """

        if not field_id:
            print("No valid identifier given")

        # Construct the SQL query for insertion
        query = """
            INSERT INTO {} (
                field_id, date, size, bbch_phase, bbch_sim,
                bsc_data, bsc_interp_data, bsc_valid,
                coh_data, coh_interp_data, coh_valid,
                s2_data, s2_interp_data, s2_valid,
                temp_min, temp_max, temp_mean, precip
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """.format(table_name)

        # Execute the query with the provided data
        db_cursor.execute(query, (
            field_id, date, size, bbch_phase, bbch_sim,
            bsc_data, bsc_interp_data, bsc_valid,
            coh_data, coh_interp_data, coh_valid,
            s2_data, s2_interp_data, s2_valid,
            temp_min, temp_max, temp_mean, precip
        ))

        # Commit the transaction
        db_connector.commit()

    @staticmethod
    def insert_complete_test_row(db_cursor, db_connector):
        """
        Insert a complete test row into the field_day table.

        Parameters:
            db_cursor (psycopg2.extensions.cursor): The database cursor object.
            db_connector (psycopg2.extensions.connection): The database connection object.
        """

        # Test variable content
        field_id = 1
        date = '2024-04-15 12:00:00'  # Example timestamp
        size = 10
        bbch_phase = 3
        bbch_sim = True
        bsc_data = 'bsc_raster_001.tif'
        bsc_interp_data = 'bsc_interp_raster_001.tif'
        bsc_valid = True
        coh_data = 'coh_raster_001.tif'
        coh_interp_data = 'coh_interp_raster_001.tif'
        coh_valid = True
        s2_data = 's2_raster_001.tif'
        s2_interp_data = 's2_interp_raster_001.tif'
        s2_valid = True
        temp_min = 10.5
        temp_max = 25.3
        temp_mean = 17.8
        precip = 5.2

        AccessSql.insert_complete_row(db_cursor, db_connector,
                            field_id, date, size, bbch_phase, bbch_sim,
                            bsc_data, bsc_interp_data, bsc_valid,
                            coh_data, coh_interp_data, coh_valid,
                            s2_data, s2_interp_data, s2_valid,
                            temp_min, temp_max, temp_mean, precip
                            )

    @staticmethod
    def enter_partial_row(db_cursor, db_connector, table_name, field_id=None, date=None, size=None, bbch_phase=None, bbch_sim=None,
                          bsc_data=None, bsc_interp_data=None, bsc_valid=None,
                          coh_data=None, coh_interp_data=None, coh_valid=None,
                          s2_data=None, s2_interp_data=None, s2_valid=None,
                          temp_min=None, temp_max=None, temp_mean=None, precip=None):
        """
        Insert a partial row into the field_day table.

        Parameters:
            db_cursor (psycopg2.extensions.cursor): The database cursor object.
            db_connector (psycopg2.extensions.connection): The database connection object.
            table_name: The name of the table to enter a row.
            field_id (int, optional): The identifier of the field.
            date (str, optional): The date of the observation.
            size (int, optional): The size of the field.
            bbch_phase (int, optional): The BBCH phase.
            bbch_sim (bool, optional): Indicates whether the BBCH phase is simulated.
            bsc_data (str, optional): The name of the BSC raster.
            bsc_interp_data (str, optional): The name of the interpolated BSC raster.
            bsc_valid (bool, optional): Indicates whether the BSC data is valid.
            coh_data (str, optional): The name of the COH raster.
            coh_interp_data (str, optional): The name of the interpolated COH raster.
            coh_valid (bool, optional): Indicates whether the COH data is valid.
            s2_data (str, optional): The name of the S2 raster.
            s2_interp_data (str, optional): The name of the interpolated S2 raster.
            s2_valid (bool, optional): Indicates whether the S2 data is valid.
            temp_min (float, optional): The minimum temperature.
            temp_max (float, optional): The maximum temperature.
            temp_mean (float, optional): The mean temperature.
            precip (float, optional): The precipitation.
        """

        if not field_id:
            print("No valid identifier given")

        # Retrieve object by ID from the database. Only enter row that have not already been entered.
        db_cursor.execute(f"SELECT * FROM public.{table_name} WHERE field_id = %s AND date = %s", (field_id, date))
        db_row = db_cursor.fetchone()
        if db_row:
            print("Data row with field_id " + str(field_id) + " and date:" + str(date) + " already entered.")
            return
        else:
            print("Data row with field_id " + str(field_id) + " and date:" + str(date) + " entered.")

        parameter_values = [value for value in locals().values()]
        valid_parameter_values = [value for value in parameter_values if value is not None]

        valid_parameter_list = [column for column, value in locals().items() if value is not None]
        query_parameter_markers = ["%s" for _ in range(len(valid_parameter_values))]

        valid_parameter_values = valid_parameter_values[3:]
        valid_parameter_list = valid_parameter_list[3:len(valid_parameter_values) + 3]
        query_parameter_markers = query_parameter_markers[3:]

        raster_bsc = raster_coh = raster_s2 = raster_s2_interp = None

        if bsc_data:
            raster_bin = AccessSql.read_geotiff_bin(bsc_data)
            bsc_rast_query = """ST_FromGDALRaster(%s::bytea)"""

            # Replace path with raster binary at same position
            valid_parameter_values = AccessSql.insert_list_at_item(valid_parameter_values, bsc_data, [raster_bin])

            # Get index of parameter position
            index = valid_parameter_list.index("bsc_data")

            # Replace simple parameter name in query with query Postgis command to load raster binary as GDAL Raster
            query_parameter_markers.pop(index)
            query_parameter_markers.insert(index, bsc_rast_query)

        if coh_data:
            raster_bin = AccessSql.read_geotiff_bin(coh_data)
            coh_rast_query = """ST_FromGDALRaster(%s::bytea)"""

            # Replace path with raster binary at same position
            valid_parameter_values = AccessSql.insert_list_at_item(valid_parameter_values, coh_data, [raster_bin])

            # Get index of parameter position
            index = valid_parameter_list.index("coh_data")

            # Replace simple parameter name in query with query Postgis command to load raster binary as GDAL Raster
            query_parameter_markers.pop(index)
            query_parameter_markers.insert(index, coh_rast_query)

        if s2_data:
            raster_bin = AccessSql.read_geotiff_bin(s2_data)
            s2_rast_query = """ST_FromGDALRaster(%s::bytea)"""

            # Replace path with raster binary at same position
            valid_parameter_values = AccessSql.insert_list_at_item(valid_parameter_values, s2_data, [raster_bin])

            # Get index of parameter position
            index = valid_parameter_list.index("s2_data")

            # Replace simple parameter name in query with query Postgis command to load raster binary as GDAL Raster
            query_parameter_markers.pop(index)
            query_parameter_markers.insert(index, s2_rast_query)

        if s2_interp_data:
            raster_bin = AccessSql.read_geotiff_bin(s2_interp_data)
            s2_rast_query = """ST_FromGDALRaster(%s::bytea)"""

            # Replace path with raster binary at same position
            valid_parameter_values = AccessSql.insert_list_at_item(valid_parameter_values, s2_interp_data, [raster_bin])

            # Get index of parameter position
            index = valid_parameter_list.index("s2_interp_data")

            # Replace simple parameter name in query with query Postgis command to load raster binary as GDAL Raster
            query_parameter_markers.pop(index)
            query_parameter_markers.insert(index, s2_rast_query)

        # Construct the SQL query for insertion with only non-None parameters
        query = """
            INSERT INTO public.{} (
                {}
            )
            VALUES ({})""".format(table_name, ", ".join(valid_parameter_list),", ".join(query_parameter_markers))

        # Execute the query with the provided data
        db_cursor.execute(query, valid_parameter_values)

        # Commit the transaction
        db_connector.commit()

    @staticmethod
    def update_partial_row(db_cursor, db_connector, table_name, ras_as_bin=False, field_id=None, date=None, size=None,
                           bbch_phase=None, bbch_sim=None,
                           bsc_data=None, bsc_interp_data=None, bsc_valid=None,
                           coh_data=None, coh_interp_data=None, coh_valid=None,
                           s2_data=None, s2_interp_data=None, s2_valid=None,
                           temp_min=None, temp_max=None, temp_mean=None, precip=None):

        """
        Update a partial row in the field_day table.

        Parameters:
            db_cursor (psycopg2.extensions.cursor): The database cursor object.
            db_connector (psycopg2.extensions.connection): The database connection object.
            ras_as_bin: This is a flag to inform if the passed raster is already transformed into binary or solely the path
            to a geotiff file.
            table_name: The name to update the row.
            field_id (int, optional): The identifier of the field.
            date (str, optional): The date of the observation.
            size (int, optional): The size of the field.
            bbch_phase (int, optional): The BBCH phase.
            bbch_sim (bool, optional): Indicates whether the BBCH phase is simulated.
            bsc_data (str, optional): The name of the BSC raster.
            bsc_interp_data (str, optional): The name of the interpolated BSC raster.
            bsc_valid (bool, optional): Indicates whether the BSC data is valid.
            coh_data (str, optional): The name of the COH raster.
            coh_interp_data (str, optional): The name of the interpolated COH raster.
            coh_valid (bool, optional): Indicates whether the COH data is valid.
            s2_data (str, optional): The name of the S2 raster.
            s2_interp_data (str, optional): The name of the interpolated S2 raster.
            s2_valid (bool, optional): Indicates whether the S2 data is valid.
            temp_min (float, optional): The minimum temperature.
            temp_max (float, optional): The maximum temperature.
            temp_mean (float, optional): The mean temperature.
            precip (float, optional): The precipitation.
        """

        if not field_id or not date:
            print("No valid identifiers field_id and date given to identify row to update.")

        # Move this to sql class for all raster data types.
        raster_bsc = raster_coh = raster_s2 = raster_s2_interp = None
        if bsc_data:
            raster_bsc = bsc_data if ras_as_bin else AccessSql.read_geotiff_bin(bsc_data)

        if coh_data:
            raster_coh = coh_data if ras_as_bin else AccessSql.read_geotiff_bin(coh_data)

        if s2_data:
            raster_s2 = s2_data if ras_as_bin else AccessSql.read_geotiff_bin(s2_data)

        if s2_interp_data:
            raster_s2_interp = AccessSql.read_geotiff_bin(s2_interp_data)

        # Construct the SQL query for updating the remaining columns
        update_query2 = """
                UPDATE {}
                SET 
                    date = COALESCE(%s, date), 
                    size = COALESCE(%s, size), 
                    bbch_phase = COALESCE(%s, bbch_phase), 
                    bbch_sim = COALESCE(%s, bbch_sim), 
                    bsc_data = COALESCE(ST_FromGDALRaster(%s::bytea), bsc_data), 
                    bsc_interp_data = COALESCE(ST_FromGDALRaster(%s::bytea), bsc_interp_data), 
                    bsc_valid = COALESCE(%s, bsc_valid), 
                    coh_data = COALESCE(ST_FromGDALRaster(%s::bytea), coh_data), 
                    coh_interp_data = COALESCE(ST_FromGDALRaster(%s::bytea), coh_interp_data), 
                    coh_valid = COALESCE(%s, coh_valid), 
                    s2_data = COALESCE(ST_FromGDALRaster(%s::bytea), s2_data), 
                    s2_interp_data = COALESCE(ST_FromGDALRaster(%s::bytea), s2_interp_data), 
                    s2_valid = COALESCE(%s, s2_valid), 
                    temp_min = COALESCE(%s, temp_min), 
                    temp_max = COALESCE(%s, temp_max), 
                    temp_mean = COALESCE(%s, temp_mean), 
                    precip = COALESCE(%s, precip)
                WHERE field_id = %s AND date = %s  -- Add condition to match the specific entry
            """.format(table_name)

        # Execute the update query with the provided data
        db_cursor.execute(update_query2, (
            date, size, bbch_phase, bbch_sim,
            raster_bsc, bsc_interp_data, bsc_valid,
            raster_coh, coh_interp_data, coh_valid,
            raster_s2, raster_s2_interp, s2_valid,
            temp_min, temp_max, temp_mean, precip,
            field_id, date  # Condition to match the specific entry
        ))

        print("Rows affected:" + str(db_cursor.rowcount))

        # Commit the transaction
        db_connector.commit()

    @staticmethod
    def delete_rows_by_id(db_connector, db_cursor, table_name, record_id):
        """
        Deletes all rows in the specified table where the id matches the given record_id.
        Be careful when using this!!!

        Parameters:
            db_cursor:
            db_connector:
            table_name: The name of the table.
            record_id: The ID value to match for deletion.

        Returns:
            int: The number of rows deleted.
        """
        try:

            # Build the query safely using psycopg2.sql to prevent SQL injection
            query = sql.SQL("DELETE FROM {table} WHERE field_id = %s").format(
                table=sql.Identifier(table_name)
            )

            # Execute the query
            db_cursor.execute(query, (record_id,))

            # Get the number of rows deleted
            rows_deleted = db_cursor.rowcount

            # Commit the transaction
            db_connector.commit()

            return rows_deleted

        except psycopg2.Error as e:
            print(f"Error: {e}")
            if db_connector:
                db_connector.rollback()
            return 0

    @staticmethod
    def connect_and_filter_by_complete(bsc, coh, s2, s2_invalid):
        """
            This connects to database and filters out all rows from field_day table as following:
            - Raster data is complete and more than 50% of the pixel are valid.
        """
        rows = None
        db_connector = None

        time.sleep(3)
        try:
            db_connector = psycopg2.connect(database="agriRef", user="postgres", host="localhost",
                                            password="sentinel22", port="5432")

            cursor = db_connector.cursor()
            query = "SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL'"
            cursor.execute(query)

            rows = AccessSql.filter_field_day(db_connector, bsc, coh, s2, s2_invalid)

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

        finally:
            # Close the cursor and connection
            if db_connector:
                db_connector.close()
                print("PostgreSQL connection is closed")
                return rows

    @staticmethod
    def filter_field_day(db_connector, bsc, coh, s2, s2_invalid):
        """
            This filters out all rows from field_day table as following:
            - Raster data is complete and more than 50% of the pixel are valid.

            Parameters:
                db_connector (psycopg2.extensions.connection): The database connection object.
                s2_invalid: The flag to define if s2 is retrieved with less than 50% valid pixel.
                s2: The flag to define if s2 radar data must be available in table.
                coh: The flag to define if coh radar data must be available in table.
                bsc: The flag to define if bsc radar data must be available in table.
        """

        cursor = db_connector.cursor()

        # Define the SQL query
        query_start = f"""
            SELECT field_id, date, size, bbch_phase, bbch_sim, 
                        ST_AsGDALRaster(bsc_data, 'GTIFF') AS bsc_data,
                        ST_AsGDALRaster(bsc_interp_data, 'GTIFF') AS bsc_interp_data,
                        bsc_valid,
                        ST_AsGDALRaster(coh_data, 'GTIFF') AS coh_data,
                        ST_AsGDALRaster(coh_interp_data, 'GTIFF') AS coh_interp_data,
                        coh_valid,
                        ST_AsGDALRaster(s2_data, 'GTIFF') AS s2_data,
                        ST_AsGDALRaster(s2_interp_data, 'GTIFF') AS s2_interp_data,
                        s2_valid, temp_min, temp_max, temp_mean, precip
            FROM public.field_day_c
            WHERE bbch_phase IS NOT NULL
                AND bbch_phase > -1
              AND precip IS NOT NULL
              AND temp_mean IS NOT NULL
        """

        # Conditional clauses based on function parameters
        conditional_clauses = []

        if bsc:
            conditional_clauses.append("bsc_data IS NOT NULL AND bsc_valid = TRUE")
        if coh:
            conditional_clauses.append("coh_data IS NOT NULL AND coh_valid = TRUE")
        if s2 and not s2_invalid:
            conditional_clauses.append("s2_data IS NOT NULL AND s2_valid = TRUE")
        if s2 and s2_invalid:
            conditional_clauses.append("s2_data IS NOT NULL")

        # Combine base query with conditional clauses
        if conditional_clauses:
            full_query = query_start + " AND " + " AND ".join(conditional_clauses)
        else:
            full_query = query_start

        query = sql.SQL(full_query)

        # Execute the query
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()

        print("The amount of rows with all values valid is: " + str(len(rows)))

        # Print or process the fetched rows as needed
        for row in rows:
            field_id, date, size, bbch_phase, bbch_sim, bsc_data, bsc_interp_data, bsc_valid, \
                coh_data, coh_interp_data, coh_valid, s2_data, s2_interp_data, s2_valid, \
                temp_min, temp_max, temp_mean, precip = row

            print("Field id: " + str(field_id if field_id else -1) + ", date: " + str(date if date else -1) + ", bbch : ",
                  str(bbch_phase if bbch_phase is not None else -1))

        return rows

    @staticmethod
    def get_valid_bbch_phase(cursor, field_id: int, date: str) -> Optional[int]:
        """
        Check the preceding and following entries in the table to find a valid bbch_phase value.

        Parameters:
        - cursor: A psycopg2 cursor object.
        - field_id: The field ID to check.
        - date: The date to check.

        Returns:
        - A valid bbch_phase value if found, otherwise None.
        """
        preceding_query = """
            SELECT bbch_phase FROM public.field_day_c
            WHERE field_id = %s AND date < %s
            AND bbch_phase IS NOT NULL
            ORDER BY date DESC LIMIT 1
        """
        following_query = """
            SELECT bbch_phase FROM public.field_day_c
            WHERE field_id = %s AND date > %s
            AND bbch_phase IS NOT NULL
            ORDER BY date ASC LIMIT 1
        """

        cursor.execute(preceding_query, (field_id, date))
        preceding_row = cursor.fetchone()

        cursor.execute(following_query, (field_id, date))
        following_row = cursor.fetchone()

        if preceding_row and preceding_row['bbch_phase'] is not None:
            return preceding_row['bbch_phase']
        elif following_row and following_row['bbch_phase'] is not None:
            return following_row['bbch_phase']
        else:
            return None

    @staticmethod
    def fetch_bbch_extended_rows(db_connector, bsc: bool, coh: bool, s2: bool) -> List[Tuple[Any]]:
        """
        Fetch rows from the field_day table based on the given conditions and process them to handle NULL bbch_phase values.

        Parameters:
        - db_connector: A psycopg2 connection object to the PostgreSQL database.
        - bsc: Boolean flag to filter rows with valid bsc_data.
        - coh: Boolean flag to filter rows with valid coh_data.
        - s2: Boolean flag to filter rows with valid s2_data.

        Returns:
        - List of tuples containing the processed rows.
        """
        try:
            cursor = db_connector.cursor(cursor_factory=DictCursor)

            base_query = f"""
                SELECT field_id, date, size, bbch_phase, bbch_sim, 
                       ST_AsGDALRaster(bsc_data, 'GTIFF') AS bsc_data,
                       ST_AsGDALRaster(bsc_interp_data, 'GTIFF') AS bsc_interp_data,
                       bsc_valid,
                       ST_AsGDALRaster(coh_data, 'GTIFF') AS coh_data,
                       ST_AsGDALRaster(coh_interp_data, 'GTIFF') AS coh_interp_data,
                       coh_valid,
                       ST_AsGDALRaster(s2_data, 'GTIFF') AS s2_data,
                       ST_AsGDALRaster(s2_interp_data, 'GTIFF') AS s2_interp_data,
                       s2_valid, temp_min, temp_max, temp_mean, precip
                FROM public.field_day_c
                WHERE precip >= 0
                  AND temp_mean >= 0
            """

            conditional_clauses = []

            if bsc:
                conditional_clauses.append("bsc_data IS NOT NULL AND bsc_valid = TRUE")
            if coh:
                conditional_clauses.append("coh_data IS NOT NULL AND coh_valid = TRUE")
            if s2:
                conditional_clauses.append("s2_data IS NOT NULL AND s2_valid = TRUE")

            if conditional_clauses:
                base_query += " AND " + " AND ".join(conditional_clauses)

            cursor.execute(base_query)
            rows = cursor.fetchall()

            processed_rows = []

            for row in rows:
                if row['bbch_phase'] is None:
                    new_bbch_phase = AccessSql.get_valid_bbch_phase(cursor, row['field_id'], row['date'])
                    if new_bbch_phase is not None:
                        row['bbch_phase'] = new_bbch_phase
                processed_rows.append(row)

            cursor.close()
            return processed_rows

        except psycopg2.Error as e:
            print(f"Error fetching rows from database: {e}")
            return []

    @staticmethod
    def query_raster_by_coordinate(db_cursor, table_name, raster_column, lon, lat, srid):
        # TODO: Test this function
        """
            Execute a SQL query to filter entries in the field_day table based on a given coordinate.

            Parameters:
                - db_cursor (psycopg2.extensions.cursor): The database cursor object.
                - table_name: The name of the table to query.
                - raster_column: The name of the raster column in the table.
                - lon (float): The longitude coordinate.
                - lat (float): The latitude coordinate.
                - srid (int): The spatial reference identifier (SRID) of the coordinate system.

            Returns:
                list: A list of tuples containing the filtered entries.
        """

        # Convert SRID coordinates to table coordinates 25832
        db_cursor.execute(f"SELECT ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), {srid}), 25832));", (lon, lat))
        transformed_coords = db_cursor.fetchone()

        if transformed_coords:
            # Extract the transformed coordinates
            transformed_lon, transformed_lat = transformed_coords

            # Construct the SQL query with the transformed coordinates
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE {raster_column} IS NOT NULL
                AND ST_Intersects({raster_column}, ST_SetSRID(ST_MakePoint(%s, %s), 25832));
            """

            # Execute the query with the transformed coordinates
            db_cursor.execute(query, (transformed_lon, transformed_lat))

            # Fetch the results
            results = db_cursor.fetchall()

            return results
        else:
            print("Failed to transform coordinates.")
            return []

    @staticmethod
    def query_by_geojson_polygon(table_name, raster_column, geojson_file_path, srid):
        """
            Query a PostGIS table by a GeoJSON polygon from a file.

            Parameters:
                - db_cursor (psycopg2.extensions.cursor): The database cursor object.
                - table_name: The name of the table to query.
                - raster_column: The name of the raster column in the table.
                - geojson_file_path: Path to the GeoJSON file containing the polygon.
                - srid: The SRID of the GeoJSON polygon.

            Returns:
                - query results.
        """

        db_connector, db_cursor = AccessSql.create_db_connection()

        # This depends on the format and content of the geojson file.
        polygon_wkt = geo.load_wkt_from_geojson(geojson_file_path)
        polygon_wkt = polygon_wkt.replace(" ", "", 1)

        # Convert SRID coordinates to table coordinates 25832
        query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE {raster_column} IS NOT NULL
                    AND ST_Intersects(
                                     ST_Transform(
                                                  GeomFromEWKT('SRID={srid};{polygon_wkt}'), 
                                                  25832
                                                  ),
                                     {raster_column});
                """

        db_cursor.execute(query)

        results = db_cursor.fetchall()

        for row in results:
            field_id, geom, startdate, enddate, nuar, crop_type, buff_distm, size = row
            print("Field id: " + str(field_id) + ", date range: [" + str(startdate) + "," + str(enddate) + "], crop type : ",
                str(crop_type) + ", size in sqm: " + str(size))

        return results

# ------------------Helper methods-------------------------------------

    @staticmethod
    def save_raster_as_geotiff(raster_data, output_path):
        """
        Save raster data as GeoTIFF file.

        Parameters:
        - raster_data: Raster data object (gdal.Dataset)
        - output_path: Output file path for saving GeoTIFF
        """
        try:
            with open(output_path, "wb") as f:
                f.write(raster_data)
            print(f"GeoTIFF saved successfully: {output_path}")
        except Exception as e:
            print(f"Error saving GeoTIFF: {e}")

    @staticmethod
    def process_row_to_geotiffs(row, path_to_geotiff):
        """
        Process a fetched row from database to save raster data as GeoTIFFs.

        Parameters:
        - row: Tuple containing all column values of the fetched row.
        - path_to_geotiff: Absolute path to save row to.

        """
        if not row:
            print("No data found for the given ID and date.")
            return

        try:
            field_id, date, size, bbch_phase, bbch_sim, bsc_data, bsc_interp_data, bsc_valid, \
                coh_data, coh_interp_data, coh_valid, s2_data, s2_interp_data, s2_valid, \
                temp_min, temp_max, temp_mean, precip = row

            # List of tuples for raster data (column name, raster object, output file name)
            raster_items = [
                ("bsc_data", bsc_data, "bsc_data.tif"),
                ("bsc_interp_data", bsc_interp_data, "bsc_interp_data.tif"),
                ("coh_data", coh_data, "coh_data.tif"),
                ("coh_interp_data", coh_interp_data, "coh_interp_data.tif"),
                ("s2_data", s2_data, "s2_data.tif"),
                ("s2_interp_data", s2_interp_data, "s2_interp_data.tif")
            ]

            for column_name, raster_obj, output_filename in raster_items:
                if raster_obj and isinstance(raster_obj, memoryview):

                    # Create an in-memory file with rasterio
                    with MemoryFile(raster_obj.tobytes()) as memfile:
                        with memfile.open() as rasterio_raster:
                            InterpolateGeotiffs.print_raster_info(rasterio_raster)

                            # To save raster as GeoTIFF set the path to save to and active code
                            AccessSql.save_raster_as_geotiff(rasterio_raster, path_to_geotiff)
                else:
                    print(f"No raster data found for {column_name}.")

        except Exception as e:
            print(f"Error processing row to GeoTIFFs: {e}")

    @staticmethod
    def insert_list_at_item(old_list, item, new_list):
        """
            Insert a list at the position of a list item with specific content.
            The item is removed from the list.

            Parameters:
                old_list (list): The original list.
                item: The specific content to find and replace.
                new_list (list): The list to insert at the position of the found item.

            Returns:
                list: The modified list.
        """

        try:
            # Find the index of the item
            index = old_list.index(item)

            # Remove the item from the list
            old_list.pop(index)

            # Insert the new list at the index
            old_list[index:index] = new_list

        except ValueError:
            print(f"Item '{item}' not found in the list")
        return old_list

    @staticmethod
    def read_geotiff_bin(filepath):
        """
        Read the GeoTIFF file and return the raster as binary.
        """
        with open(filepath, 'rb') as f:
            return psycopg2.Binary(f.read())

    @staticmethod
    def create_raster_query_comp(raster_bsc, double_bit):
        # This method was not fully successful. There were problems with the band content!!!
        # Use of ST_FromGDALRaster(%s::bytea) instead.

        """This method is to write raster data to a table row by band with:
        # ST_MakeEmptyRaster();
        # ST_AddBand();
        # ST_SetValues(); is not working correctly. It seems the offset parameters are not being set properly.
        # The resulting data in the table only contains 0s.
        """

        buffer_band = raster_bsc.GetRasterBand(1).ReadRaster(0, 0, raster_bsc.RasterXSize, raster_bsc.RasterYSize)
        float_band = np.frombuffer(buffer_band, dtype=np.float32 if double_bit else np.float16)
        float_band_nested = float_band.reshape(raster_bsc.RasterYSize, raster_bsc.RasterXSize).tolist()

        nodata_value = raster_bsc.GetRasterBand(1).GetNoDataValue()

        list_of_param = [raster_bsc.RasterXSize, # width
                         raster_bsc.RasterYSize, # height
                         float(raster_bsc.GetGeoTransform()[0]),  # X coordinate of upper-left corner
                         float(raster_bsc.GetGeoTransform()[3]),  # Y coordinate of upper-left corner
                         float(raster_bsc.GetGeoTransform()[1]),  # Pixel width
                         float(raster_bsc.GetGeoTransform()[5]),  # Pixel height
                         float(raster_bsc.GetGeoTransform()[2]),  # Rotation
                         float(raster_bsc.GetGeoTransform()[4]),  # Rotation
                         25832,  # srid
                         1,  # Band index for AddBand
                         0,  # Initialvalue for AddBand
                         nodata_value,  # NoData value
                         1,  # Band index for SetValues
                         1,  # x offset
                         1, # y offset
                         float_band_nested # Raster data to be added
                         ]

        # ST_MakeEmptyRaster(integer width, integer height, float8 upperleftx, float8 upperlefty, float8 scalex, float8 scaley, float8 skewx, float8 skewy, integer srid=unknown);
        # ST_AddBand(raster rast, integer index, text pixeltype, double precision initialvalue=0, double precision nodataval=NULL);
        # ST_SetValues(raster rast, integer columnx, integer rowy, integer width, integer height, double precision newvalue, boolean keepnodata=FALSE);

        # This works on 2 band geotiff but still only reads one band in the database.
        # Set Values only works for 1 band at a time.
        postqis_raster_com = """ST_SetValues(
                                             ST_AddBand(
                                                        ST_MakeEmptyRaster(%s, %s, %s, %s, %s, %s, %s, %s, %s),
                                                        %s, '32BF', %s, %s
                                                        ), %s, %s, %s, ARRAY[%s]::double precision[]
                                             )"""

        return postqis_raster_com, list_of_param

    @staticmethod
    def add_band_to_raster(db_cursor, raster_data, band_num, field_id, date, double_bit):

        # This method is no longer in use because it builds on create_raster_query_comp(raster_bsc, double_bit)
        # Which contains error. Try this method on raster data already contained in table to add band and test.

        # Read raster data for each band into memory buffers
        buffer_band = raster_data.GetRasterBand(band_num).ReadRaster(0, 0, raster_data.RasterXSize, raster_data.RasterYSize)
        float_band = np.frombuffer(buffer_band, dtype=np.float32 if double_bit else np.float16)
        float_band_nested = float_band.reshape(raster_data.RasterYSize, raster_data.RasterXSize).tolist()

        """
        Retrieve raster object by ID from the database.
        """
        db_cursor.execute("SELECT bsc_data FROM public.field_day_c WHERE field_id = %s AND date = %s", (field_id,date))
        raster_result = db_cursor.fetchone()

        if raster_result:
            # Execute the SQL UPDATE statement
            db_cursor.execute("""
                UPDATE field_day
                SET bsc_data = ST_SetValues(
                    bsc_data, %s, %s, %s, %s
                )
                WHERE field_id = %s;
            """, (band_num, raster_data.RasterXSize, raster_data.RasterYSize, float_band_nested, field_id))

            print("Band " + str(band_num) + " added successfully.")
        else:
            print("Band " + str(band_num) + " not added to current bsc item.")
