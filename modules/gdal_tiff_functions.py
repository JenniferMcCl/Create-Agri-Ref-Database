#--------------------------------------------------------------------------------------------------------------------------------
# Name:        gdal_tiff_functions
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
#--------------------------------------------------------------------------------------------------------------------------------

from osgeo import gdal


class GdalTiffFunctions:
    """
    This class contains methods to manipulate and update geotiffs. Meant to unify saved data before uploading to sql
    database.
    """

    @staticmethod
    def get_gdal_tiff_info(tiff_file):

        if not tiff_file.endswith(".tif"):
            return None

        rds = gdal.Open(tiff_file)
        print("The attributes are: " + str(dir(rds)))
        print("The amount of bands are: " + str(rds.RasterCount))

        print("The size is: " + str(rds.RasterXSize) + "/" + str(rds.RasterYSize))
        band1 = rds.GetRasterBand(1)
        band2 = rds.GetRasterBand(2)

        print("The attributes of bands are: " + str(dir(band1)))

        print("The color table of band 1 is: " + str(band1.GetRasterColorTable()))
        print("The color table of band 2 is: " + str(band2.GetRasterColorTable()))

        print("The meta data of band 1 are: " + str(band1.GetStatistics(0, 1)))
        print("The meta data of band 2 are: " + str(band2.GetStatistics(0, 1)))

        print("The max/min values of band 1 are: " + str(band1.GetMaximum()) + "/" + str(band1.GetMinimum()))
        print("The no data value of band 1 is: ", str(band1.GetNoDataValue()))

        print("The max/min values of band 2 are: " + str(band2.GetMaximum()) + "/" + str(band2.GetMinimum()))
        print("The no data value of band 2 is: ", str(band2.GetNoDataValue()))

    @staticmethod
    def update_nan_and_stats(tiff_file, amount_bands, nan_value=None, min_value=None):

        if not tiff_file.endswith(".tif"):
            return None

        rds = gdal.Open(tiff_file, gdal.GA_Update)

        band1 = rds.GetRasterBand(1)
        arr1 = band1.ReadAsArray()

        if nan_value is not None:
            arr1[arr1 == 0] = nan_value
            band1.SetNoDataValue(nan_value)

        if min_value is not None:
            arr1[arr1 < min_value] = nan_value

        # Use this to compute stats and save to tiff file
        band1.ComputeBandStats()
        band1.WriteArray(arr1)

        if amount_bands == 2:
            band2 = rds.GetRasterBand(2)
            arr2 = band2.ReadAsArray()

            if nan_value is not None:
                arr2[arr2 == 0] = nan_value
                band2.SetNoDataValue(nan_value)

            if min_value is not None:
                arr2[arr2 < min_value] = nan_value

            # Use this to compute stats and save to tiff file
            band2.ComputeBandStats()
            band2.WriteArray(arr2)

        if amount_bands == 1:
            return [rds.RasterXSize, rds.RasterYSize], band1.GetStatistics(0, 1)
        else:
            return [rds.RasterXSize, rds.RasterYSize], band1.GetStatistics(0, 1), band2.GetStatistics(0, 1)

    @staticmethod
    def scale_tiff_arr_to_range(tiff_file, old_min, old_max, new_min, new_max):

        if not tiff_file.endswith(".tif"):
            return None

        rds = gdal.Open(tiff_file, gdal.GA_Update)
        band1 = rds.GetRasterBand(1)
        band2 = rds.GetRasterBand(2)

        arr1 = band1.ReadAsArray()
        arr2 = band2.ReadAsArray()

        new_arr1 = GdalTiffFunctions.scale_deg_to_range(old_min, old_max, new_min, new_max, arr1)
        new_arr2 = GdalTiffFunctions.scale_deg_to_range(old_min, old_max, new_min, new_max, arr2)

        band1.WriteArray(new_arr1)
        band2.WriteArray(new_arr2)

    @staticmethod
    def scale_deg_to_range(old_min, old_max, new_min, new_max, old_value):
        OldRange = (old_max - old_min)
        NewValue = None
        if OldRange == 0:
            new_value = new_min
        else:
            NewRange = (new_max - new_min)
            NewValue = (((old_value - old_min) * NewRange) / OldRange) + new_min

        return NewValue

    @staticmethod
    def cloud_optimize_gtiff(input_path="", input_name="", output_path="", output_name=""):
        ds = gdal.Translate(output_path + output_name, input_path + input_name,
                            options="-of COG -co BLOCKSIZE=512 -co RESAMPLING=BILINEAR -co COMPRESS=DEFLATE "
                                    "-co NUM_THREADS=60 -co TARGET_SRS=EPSG:25832")
        return ds
