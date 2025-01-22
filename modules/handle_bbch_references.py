#--------------------------------------------------------------------------------------------------------------------------------
# Name:        handle_bbch_references
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
#--------------------------------------------------------------------------------------------------------------------------------

import json, os
import csv
import datetime
import operator as op

from datetime import datetime as dt


class HandleBBCHReferences:
    """
    This is very application specific and depends on the format the reference data is delivered for BBCH stages
    for specific days, fields and types.
    """

    bbch_date_pairs = None
    bbch_average_ct = {0: [datetime.timedelta(days=0), 0]}
    bbch_min_max = {0: [datetime.timedelta(days=0), datetime.timedelta(days=0)]}
    fileHandlerWriter = None

    # This is for limiting the amount of BBCH values to 75 other than all 100
    BBCH_VALUES = list(range(0, 75))

    def __init__(self):
        dates = [""] * len(HandleBBCHReferences.BBCH_VALUES)
        self.bbch_date_pairs = dict(map(lambda i, j: (i, j), HandleBBCHReferences.BBCH_VALUES, dates))

        self.bbch_average_ct = {0: [datetime.timedelta(days=0), 0]}
        self.bbch_min_max = {0: [datetime.timedelta(days=0), datetime.timedelta(days=0)]}

        for i in range(0, 75):
            self.bbch_average_ct[i] = [datetime.timedelta(days=0), 0]

        for i in range(0, 75):
            self.bbch_min_max[i] = [datetime.timedelta(days=365),datetime.timedelta(days=0)]

    @staticmethod
    def create_all_field_polygons(self, inputFolder, outputFolder):
        input_names = os.listdir(inputFolder)
        input_names.sort()
        input_list = [inputFolder + i for i in input_names]
        input_list.sort()

        for i in range(0, len(input_list)):
            HandleBBCHReferences.__create_geojson_polygon(inputFolder, outputFolder, input_names[i])

    def create_bbch_csv_file(self, inputFolder, outputFolder, type):
        input_names = os.listdir(inputFolder)
        input_names.sort()
        input_list = [inputFolder + i for i in input_names]
        input_list.sort()

        writeArray = open(outputFolder + "bbch_values_" + type + ".csv", 'a')
        self.fileHandlerWriter = csv.writer(writeArray)
        self.fileHandlerWriter.writerow(
            ["Name", "Date", "BBCH", "NumbObs", "BBCH_id", "BBCH_avr", "BBCH_min", "BBCH_max", "Range"
             "BBCH_ct", "NumFields"])

        # Go through all files. One file per field, per year.
        field_count = 0
        for i in range(0, len(input_list)):
            entry = HandleBBCHReferences.__read_bbch_date_arrays(input_list[i], type)

            if not entry[1]:
                print("Skipping field observation:" + entry[0] + ". Entry invalid.")
                continue

            date_bbch_values = entry[1]
            bbch_values = [i[1] for i in date_bbch_values]
            bbch_values_sorted = bbch_values
            bbch_values_sorted.sort()

            if bbch_values != bbch_values_sorted:
                print("BBCH values are not subsequent.")

            # Check amount of BBCH stage 0 -> "sowing date" in second item of list
            x = [j for i in date_bbch_values for j in i]
            if op.countOf(x, 0) < 1:
                print("Start date not available.")
                continue
            elif op.countOf(x, 0) > 1:
                print("Start date has duplicate.")
                date_bbch_values = list(set(i for i in date_bbch_values))
                date_bbch_values.sort()

            first_date = date_bbch_values[0][0]
            last_date = date_bbch_values[len(date_bbch_values) - 1][0]

            timeDelta = last_date - first_date
            if timeDelta > datetime.timedelta(days=356):
                print("Timerange larger than year.")

            entry = (entry[0], date_bbch_values, entry[2])
            if not self.__write_field_observation(entry):
                continue
            field_count = field_count + 1
            self.fileHandlerWriter.writerow(["", "", "", "", "", "", "", "", "", "", field_count])

        self.fileHandlerWriter.writerow(
            ["Name", "Date", "BBCH", "NumbObs", "BBCH_id", "BBCH_avr", "BBCH_min", "BBCH_max", "Range"
             "BBCH_ct", "NumFields"])

        for i in range(0, len(self.bbch_average_ct)):

            if self.bbch_average_ct[i][1] > 0:
                bbch_avg = self.bbch_average_ct[i][0] / self.bbch_average_ct[i][1]
            else:
                bbch_avg = datetime.timedelta(days=0)
            self.fileHandlerWriter.writerow(["", "", "", "", i, bbch_avg,
                                             self.bbch_min_max[i][0],
                                             self.bbch_min_max[i][1],
                                             self.bbch_min_max[i][1] - self.bbch_min_max[i][0],
                                             self.bbch_average_ct[i][1]])

        writeArray.close()

    def __write_field_observation(self, entry):

        self.fileHandlerWriter.writerow([entry[0], "", "", entry[2], "", "", "", "", "", ""])

        bbch_values = entry[1]
        if bbch_values[0][1] == 0 and len(bbch_values) > 1:
            if bbch_values[0][0] - bbch_values[1][0] > datetime.timedelta(days=115):
                bbch_values[1] = (bbch_values[0][0] - datetime.timedelta(days=356), 0)
            elif bbch_values[1][0] - bbch_values[0][0] > datetime.timedelta(days=300):
                bbch_values[0] = (bbch_values[0][0] + datetime.timedelta(days=356), 0)

        elif bbch_values[len(bbch_values) - 1][1] == 0 and len(bbch_values) > 1:
            new_bbch_startdate = bbch_values[len(bbch_values) - 1][1]
            if bbch_values[len(bbch_values) - 1][0] - bbch_values[1][0] > datetime.timedelta(days=115):
                new_bbch_startdate = bbch_values[len(bbch_values) - 1][0] - datetime.timedelta(days=356)
            if new_bbch_startdate - bbch_values[1][0] > datetime.timedelta(days=115):
                new_bbch_startdate = new_bbch_startdate - datetime.timedelta(days=356)

            bbch_values.insert(0, (new_bbch_startdate, 0))
            bbch_values.pop()
        else:
            print("----------------Skipping field observation:" + entry[0] + ". Start date missing.-----------")
            return False

        if bbch_values[0][0] >= bbch_values[1][0] or bbch_values[1][0] - bbch_values[0][0] < datetime.timedelta(days=2):
            print("----------------Skipping field observation:" + entry[0] + ". Start date missing.-----------")
            return False

        start_datetime = bbch_values[0][0]

        for j in range(0, len(bbch_values)):
            new_datetime = bbch_values[j][0]
            current_bbch = bbch_values[j][1]

            try:
                timeDelta = new_datetime - start_datetime
            except:
                print("Wrong types")

            if (timeDelta <= datetime.timedelta(days=2) and current_bbch >= 5):
                continue
                print("Time delta too small for bbch stage according to start date.")

            # Set new maximum date for BBCH stage
            if timeDelta < self.bbch_min_max[current_bbch][0]:
                self.bbch_min_max[current_bbch][0] = timeDelta

            # Set new minimum date for BBCH stage
            if timeDelta > self.bbch_min_max[current_bbch][1]:
                self.bbch_min_max[current_bbch][1] = timeDelta

            self.fileHandlerWriter.writerow(["", new_datetime, bbch_values[j][1], "", "", "", "", "", "", ""])
            self.bbch_average_ct[int(current_bbch)] = [self.bbch_average_ct[int(current_bbch)][0] + timeDelta,
                                                       self.bbch_average_ct[int(current_bbch)][1] + 1]
        return True

    @staticmethod
    def __create_geojson_polygon(inputFolder, outputFolder, geojson):

        with open(inputFolder + geojson) as f:
            print (geojson)
            data = json.load(f)

        with open(outputFolder + geojson, "w") as f:

            if len(data["features"]) == 1:
                properties = data["features"][0]["geometry"]
                prop = str(properties).replace("'", '"')
                f.write(prop)
                f.close()

    @staticmethod
    def __tranlate_to_datetime(dateStr):
        date_format = '%Y-%m-%d'

        dateList = dateStr.split("-")
        if len(dateList[len(dateList) - 1]) == 1:
            dateList[len(dateList) - 1] = "0" + dateList[len(dateList) - 1]

        dateStr = '-'.join(dateList)

        try:
            dateTime = dt.strptime(dateStr, date_format)
            return dateTime
        except:
            print("Month " + dateStr + " as digit not valid")

        date_format = '%Y-%b-%d'

        try:
            dateTime = dt.strptime(dateStr, date_format)
            return dateTime
        except:
            dateTime = None
            print("Month " + dateStr + " as Name not valid.")

        return dateTime

    @staticmethod
    def __read_bbch_date_arrays(geojson, cropType):

        with open(geojson) as f:
            print (geojson)
            data = json.load(f)

        for i in range(len(data["features"])):
            properties = data["features"][i]["properties"]
            dateList = properties["date"]
            bbch = properties["bbch"]
            id = properties["ID"]
            jahr = properties["JAHR"]
            currentType = properties["NUTZART"]
            numObs =  properties["numbObs"]

            id = str(int(id))
            if dateList and bbch and id and jahr:
                if type(dateList) is not list:
                    dates = properties["date"].replace("[", "").replace("]", "").replace("  ", " ").replace("'",                                                                                                           "").replace(",", "")
                    dateList = dates.split(" ")

                dateList = [HandleBBCHReferences.__tranlate_to_datetime(i) for i in dateList]
                bbchs = properties["bbch"].replace("  ", " ").replace("  ", " ").replace("[ ", "").replace("]",
                                                                                                           "").replace(".", "")
                bbchList = bbchs.split(" ")
                bbchList = [int(i.replace(".", "").replace("[", "")) for i in bbchList]

                dateBbchList = list(map(lambda i,j : (i,j), dateList, bbchList))
                dateBbchList = [x for x in dateBbchList if x[0]]

                dateBbchList.sort(key=lambda x: x[0])
                return id, dateBbchList, numObs, currentType
        return id, None, None, currentType

    @staticmethod
    def calculate_median_bbch(inputFolder = "", outputFolder =""):
        dates = [""]* len(HandleBBCHReferences.BBCH_VALUES)
        bbch_date_pairs = dict(map(lambda i, j: (i, j), HandleBBCHReferences.BBCH_VALUES, dates))
        print(bbch_date_pairs)

        date1 = datetime.date(year=2018, month=1, day=1)
        date2 = datetime.date(year=int(2018), month=int(1), day=int(7))
        date3 = datetime.date(year=int(2018), month=int(1), day=int(10))
        date4 = ((date3 - date1) + (date2 - date1)) /3 + date1
        print(date4)
