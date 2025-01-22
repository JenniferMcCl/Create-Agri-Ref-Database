#--------------------------------------------------------------------------------------------------------------------------------
# Name:        update_data_with_gdal
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
#--------------------------------------------------------------------------------------------------------------------------------

import modules.gdal_tiff_functions as update
import csv, os, datetime

file_handler_writer = None
file_handler_csv = None

def update_meta_data(input_folder):
    """This function updates the metadata of all geotiff files in the given folder."""

    global file_handler_writer
    global file_handler_csv

    currentDatetime = datetime.datetime.now()
    date = ("%s%s%s" % (currentDatetime.day, currentDatetime.month, currentDatetime.year))
    time = ("%s:%s:%s" % (currentDatetime.hour, currentDatetime.minute, currentDatetime.second))

    csv_name = input_folder + "meta_data_" + date + "_" + time + ".csv"

    fileHandlerCsv = open(csv_name, 'a')
    fileHandlerWriter = csv.writer(fileHandlerCsv)

    fileHandlerWriter.writerow(["File", "Band", "SizeX", "SizeY", "Min", "Max", "Mean", "Std Dev"])

    item_list = os.listdir(input_folder)
    item_list = [input_folder + i for i in item_list]
    item_list.sort()

    for i in range(0, len(item_list)):
        tempL = item_list[i].split("/")
        entry = tempL[len(tempL) - 1]
        entryList = entry.split("_")
        currentOrbit = entryList[3]

        # This changes the content of the tiff files, deactivate if not requested. Min value sets all values below to nan.
        result = update.GdalTiffFunctions.update_nan_and_stats(item_list[i], 1, 6.9055e-41, None)
        if not result:
            return

        print("Update regular: " + item_list[i])

        fileHandlerWriter.writerow([item_list[i], "Band 1", result[0][0], result[0][1], result[1][0],
                                    result[1][1], result[1][2], result[1][3]])

        #This is for working with 2 bands as for Backscatter
        #fileHandlerWriter.writerow([item_list[i], "Band 2", result[0][0], result[0][1], result[2][0],
        #                            result[2][1], result[2][2], result[2][3]])

def main():

    input_folder = ""

    item_list = os.listdir(input_folder)
    item_list = [input_folder + i for i in item_list]
    item_list.sort()

    for i in range(1, len(item_list)):
        update_meta_data(item_list[i] + "/")

if __name__ == "__main__":
    main()