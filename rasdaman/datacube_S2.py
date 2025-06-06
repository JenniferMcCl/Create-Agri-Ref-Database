#--------------------------------------------------------------------------------------------------------------------------------
# Name:        datacube_S2
# Purpose:
#
# Author:      florian.beyer, jennifer.mcclelland
#
# Created:
# Copyright:   (c) florian.beyer
#
#--------------------------------------------------------------------------------------------------------------------------------


import requests
from requests.auth import HTTPBasicAuth

class DatacubeS2:

    @staticmethod
    def get_S2_imagery(polygon, layer, date, user, pw, host, epsg = 32632, band1='NIR10', band2='R', band3='G', band_subset=True, printout=False , get_query=False):
        """
        get analysis-ready Copernicus Sentinel-2 reflectance data (cloud masked, bottom-of-atmosphere) from JKI-CODE-DE DataCube

        PARAMETERS:
            polygon (str): polygon boundaries as WKT string (https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry)
            layer (str): layer name of the data cube (coverage ID)
            date (str): YYYY-MM-DD
            user (str): credentials username
            pw (str): credentials password
            host (str): host adress of data cube service
            epsg (int): defines coordinate reference system (CRS) of the input (easting and northing) and output coordinates. Defaults to 32632.
            band1 (str): band name (default: NIR10)
            band2 (str): band name (default: R)
            band3 (str): band name (default: G)
            band_subset (bool(opt)): If True, request only returns given bands (band1 - band3). Defaults to True.
            printout (bool(opt)): If True, some information will be printed about the success of request. Defaults to False.
            get_query (bool(opt)): If True, final WCS URL will be printed. Defaults to False.


        RETURNS:
            response (requests.models.Response): binary response object, in which the cropped S2-image is stored (https://requests.readthedocs.io/en/latest/)

        """

        try:
            # set WCS query parameters
            service = '?&SERVICE=WCS'
            version = '&VERSION=2.0.1'
            request = '&REQUEST=GetCoverage'
            coverage_id = '&COVERAGEID=' + layer # set name of RASDAMAN layer here
            subset_time = '&SUBSET=ansi(\"' + date + '\")'
            subsetting_crs = '&subsettingCrs=http://ows.rasdaman.org/def/crs/EPSG/0/' + str(epsg) # your EPSG code

            # This is how to use the rasdaman CURTAIN option: See documentation "https://doc.rasdaman.org/04_ql-guide.html?highlight=curtain"
            # polygon = ('CURTAIN(projection(E,N),POLYGON((427799.8724237755 205589111.631958339, '
            #            '20427789.6776472116 205589104.982396325, 20%20427760.1110817989 205589153.700700263,'
            #            ' 20427836.4519902036 205589142.348655859, 20427799.8724237755 205589111.631958339)))')

            clip = '&CLIP='+polygon
            output_crs = '&outputCrs=http://ows.rasdaman.org/def/crs/EPSG/0/' + str(epsg) # your EPSG code
            encode_format = '&FORMAT=image/tiff'
            rangesubset = '&RANGESUBSET='+band1+','+band2+','+band3

            # This is to include all bands
            # rangesubset = '&RANGESUBSET=01_Blue,02_Green,03_Red,04_RE1,05_RE2,06_RE3,07_NIR10,08_NIR20,09_SWIR1,10_SWIR2'

            # build query string with time subset
            query = (host + service + version + request + coverage_id + subset_time + subsetting_crs + clip +
                     output_crs + encode_format)

            if band_subset == True:
                query = query + rangesubset
            if get_query == True:
                print(query)

            query2 = ("https://code-de.rasdaman.com/rasdaman/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage"
                     "&COVERAGEID=S2_GermanyGrid&SUBSET=ansi('2019-01-01')&subsettingCrs=http://ows.rasdaman.org/def/crs/EPSG/0/32632&CLIP=POLYGON((608558 5787080, 616739 5787455, 617189 5780348, 609195 5783083, 608558 5787080))&outputCrs=http://ows.rasdaman.org/def/crs/EPSG/0/32632&FORMAT=image/tiff&RANGESUBSET=NIR10,R,G")

            # run WCS query
            response = requests.get(query, auth=HTTPBasicAuth(user, pw))

            # check if query successful
            if response.status_code == 200: # status code 200 means request wasd successful
                if printout==True:
                    print('request was sucessfull! Request Status: {}'.format(response.status_code))
                return response
            elif response.status_code == 404: # status code 404 means bad request
                if printout==True:
                    print('bad request! Request Status: {}'.format(response.status_code))
                    print('response content: ', response.text)
                return response
            else:
                if printout==True:
                    print('something went wrong. Request was answered with request code: {}. URL: {}'.format(response.status_code, response.url))
                    print('response content: ', response.text)
                else:
                    pass
                return response

        except Exception as e:
            print('something went wrong: {}'.format(e))