# --------------------------------------------------------------------------------------------------------------------------------
# Name:        field_series_creator
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------

from modules.rasdaman_request import RasdamanRequest
from rasdaman.datacube_S2 import DatacubeS2
from rasdaman.credentials import Credentials
import modules.geo_position as geo

import tifffile as reader
import os, csv, json, rasterio, io, numpy
from rasterio.mask import mask


class FieldSeriesCreator:
    """
    This works on Sentinel-1 data already created and saved on hard drive. Sentinel 2 data is derived from the Rasdaman
    layer.
    """

    def create_S2_field_series(self, years, folder_to_fields, output_folder, area_bb, log_csv_name):
        write_Array = open(log_csv_name + ".csv""", 'a')
        file_handler_writer = csv.writer(write_Array)

        file_handler_writer.writerow(["S2 Poly Date", "Current Field Clip", "Relative Nan",
                                      "Valid S2 Date Pixel", "File Size", "Valid Field Pixel"])

        # Get list of field polygons in folder by name
        field_items = os.listdir(folder_to_fields)
        field_items.sort()

        # Create array of field polygon paths
        geojson_list = FieldSeriesCreator.get_geojson_list(folder_to_fields, field_items)

        # get phases from previous year
        days = RasdamanRequest.get_all_dates(years[0])
        for i in range(1, len(years)):
            days = days + RasdamanRequest.get_all_dates(years[i])

        poly = geo.transfer_geom(area_bb, 25832, 25832)
        polygon = str(poly.wkt).replace(' (', '(')

        for k in range(0, len(days)):
            # requesting DataCube

            # example day with no clouds in RLP_1 area
#            days[k] = "2018-06-02"
            img = DatacubeS2.get_S2_imagery(
                polygon=polygon,
                layer='S2_GermanyGrid',
                date=days[k],
                user=Credentials.ras_cde_user,
                pw=Credentials.ras_cde_pw,
                host=Credentials.ras_cde_host,
                epsg=25832,
                band1='NIR10',
                band2='R',
                band3='G',
                band_subset=False,
                printout=False,
                get_query=False
            )

            print("Processing S2 date: " + str(days[k]))

            try:
                with rasterio.open(io.BytesIO(img.content), 'r', nodata=0) as src:

                    valid = RasdamanRequest().check_valid_non_zero(src, 0)
                    if valid[1] == 0.0:
                        continue

                    file_handler_writer.writerow([days[k], "", "", valid[1], "", ""])

                    for j in range(0, len(geojson_list)):
                        field_name = field_items[j].replace(".geojson", "")
                        field_output_folder = output_folder + field_name

                        date = days[k].replace("-", "")
                        name_for_clip = date + "_S2_" + field_name

                        FieldSeriesCreator.clip_to_aoi(src, geojson_list[j], name_for_clip,
                                         field_output_folder,
                                         file_handler_writer, None, True)

            except rasterio.errors.RasterioIOError as e:
                print("Error opening raster file:", e)
                print(str(days[k]) + " could not be opened.")

    def create_S1_field_series(self, folder_to_data, folder_to_fields, output_folder, log_csv_name):

        # Access all tiff data items in folder
        data_items = os.listdir(folder_to_data)
        data_items.sort()

        write_Array = open(log_csv_name + ".csv""", 'a')
        file_handler_writer = csv.writer(write_Array)
        file_handler_writer.writerow(["Scene/Field", "Current Scene/ Current Field", "Relative Nan",
                                      "Relative Zero", "File Size", "Field in Scene"])

        self.clip_all_S1_fields(folder_to_fields, data_items, folder_to_data, output_folder, file_handler_writer)

    def clip_all_S1_fields(self, folder_to_fields, data_items, folder_to_data, output_folder, file_handler_writer):

        # Get list of field polygons in folder by name
        field_items = os.listdir(folder_to_fields)
        field_items.sort()

        # Create array of field polygon paths
        geojson_list = FieldSeriesCreator.get_geojson_list(folder_to_fields, field_items)

        for i in range(0, len(data_items)):

            data_item_path = folder_to_data + data_items[i]

            # Skip any data that is not applicable by type
            if not data_item_path.endswith(".tif"):
                print(data_item_path + "is not a tif")
                continue

            current_scene_array = FieldSeriesCreator.open_tiff(data_item_path)

            if current_scene_array is None:
                file_handler_writer.writerow(["Scene", data_item_path, "", 1.0, "Invalid", ""])
                continue

            relative_zero_scene, relative_nan_scene = FieldSeriesCreator.get_amount_zero_and_nan(current_scene_array)

            file_handler_writer.writerow(["Scene", data_item_path,
                                          relative_nan_scene,
                                          relative_zero_scene,
                                          os.path.getsize(data_item_path), ""])

            print("Amount Zeros/Nan values for: " + data_item_path + " are " + str(relative_zero_scene))

            opened_data_tiff = rasterio.open(data_item_path)
            print("New geojson bounds are " + str(opened_data_tiff.bounds))

            for j in range(0, len(geojson_list)):
                field_name = field_items[j].replace(".geojson", "")
                data_name = data_items[i].replace(".tif", "")
                name_for_clip = data_name + "_" + field_name
                field_output_folder = output_folder + field_name

                FieldSeriesCreator.clip_to_aoi(opened_data_tiff, geojson_list[j], name_for_clip,
                                 field_output_folder,
                                 file_handler_writer,
                                 numpy.nan, False)

    @staticmethod
    def clip_to_aoi(opened_data_tiff, geojson_poly, name_for_clip, output_folder, file_handler_writer,
                    nan_value=None, s2=False):

        # load the raster, mask it by the polygon and crop it
        try:
            field_clip_data_array, out_trans = mask(opened_data_tiff, [geojson_poly], crop = True)

            meta = opened_data_tiff.meta
            meta.update({"driver": "GTiff",
                         "height": field_clip_data_array.shape[1],
                         "width": field_clip_data_array.shape[2],
                         "transform": out_trans})

            # Set all zero values to tiff nan value for better display on map
            # This does not work for reference to S2 geotiff from Rasdaman
            if nan_value is not None and s2 == False:
                field_clip_data_array[field_clip_data_array == 0] = numpy.nan

            relative_zero_field, relative_nan_field = FieldSeriesCreator.get_amount_zero_and_nan(
                field_clip_data_array)

            path_to_clip = output_folder + "/" + name_for_clip + ".tif"

            # Only data is used with more than 90% not zero values
            if s2 and relative_zero_field > 0.9:
                file_handler_writer.writerow(["Field",
                                              name_for_clip,
                                              relative_nan_field,
                                              relative_zero_field,
                                              0,
                                              "Yes" if relative_zero_field < 1 else "No"])
                print("Relative amount zero > 0.9")
                return False

            print("Relative Nan values for: " + name_for_clip + " are " + str(relative_nan_field))

            if os.path.exists(path_to_clip):
                print("Item already created: " + path_to_clip)
                return False

            # Field folder is only created if there is a valid item to place inside.
            if not os.path.exists(output_folder):
                os.mkdir(output_folder)

            with rasterio.open(path_to_clip, 'w', **meta) as dest1:
                for i in range(field_clip_data_array.shape[0]):
                    dest1.write(field_clip_data_array[i], i + 1)

            print("Created item: " + path_to_clip)

            file_handler_writer.writerow(["Field",
                                          name_for_clip,
                                          relative_nan_field,
                                          relative_zero_field,
                                          os.path.getsize(path_to_clip),
                                          "Yes" if relative_nan_field < 1 else "No"])

        except:
            file_handler_writer.writerow(["Field", name_for_clip, "", 1.0, "No Overlap", ""])

    @staticmethod
    def get_geojson_list(folder_to_fields, field_items):

        # Get list of field polygons path including folder name
        field_items_folder = [folder_to_fields + i for i in field_items]

        # Create array of field polygon paths
        geojson_list = []
        for j in range(0, len(field_items_folder)):
            with open(field_items_folder[j]) as data:

                if not field_items_folder[j].endswith(".geojson"):
                    print(field_items_folder[j] + " is not a geojson")
                    continue

                geoms = json.load(data)
                geojson_list.append(geoms)

        return geojson_list

    @staticmethod
    def get_S2_amount_zero_and_nan(tiff_array, nan_value=6.9055e-41):

        sizex, sizey = FieldSeriesCreator.get_size(tiff_array)
        amount_zero = (tiff_array[0] == 0).sum()
        relative_zero = amount_zero / (sizey * sizex)

        amount_nan = (tiff_array[0] == nan_value).sum()
        relative_nan = amount_nan / (sizey * sizex)

        return relative_zero, relative_nan

    @staticmethod
    def get_amount_zero_and_nan(tiff_array, nan_value=6.9055e-41):

        sizex, sizey = FieldSeriesCreator.get_size(tiff_array)
        amount_dim = tiff_array.ndim

        amount_zero = ((tiff_array if amount_dim == 2 else tiff_array[:, :, 0]) == 0).sum()
        relative_zero = amount_zero / (sizey * sizex)

        amount_nan = (((tiff_array if amount_dim == 2 else tiff_array[:, :, 0]) == nan_value).sum()
                      + ((tiff_array if amount_dim == 2 else tiff_array[:, :, 0]) == numpy.nan).sum())

        relative_nan = amount_nan / (sizey * sizex)

        return relative_zero, relative_nan

    @staticmethod
    def get_size(np_array):
        amount_dim = np_array.ndim

        heightArr = np_array.shape[0]
        widthArr = np_array.shape[1]

        #Check this for different data
        sizey = heightArr if amount_dim == 2 else np_array.shape[0]
        sizex = widthArr if amount_dim == 2 else np_array.shape[1]
        return sizex, sizey

    @staticmethod
    def open_tiff(tiff_file):
        fsize = os.path.getsize(str(tiff_file))
        #   print(src.tags())

        if fsize > 0:
            temp = None
            try:
                img = reader.imread(str(tiff_file))
            except:
                print("Corrupt File: ", str(tiff_file) + " File could not be read.")
                return temp

            temp = numpy.array(img)
            return temp
        return None

