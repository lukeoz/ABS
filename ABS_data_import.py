#!/usr/bin/env python

#-------------------------------------------------------------------------------
# Name:         ABS.Stat data import
# Purpose:      Programmatically import ABS data through the API
#
# Author:       Luke Oswald
# E-mail:       luke.oswald@sa.gov.au
#
# Created:      7 July 2020
#-------------------------------------------------------------------------------

import os
import sys
from pandasdmx import Request
from datetime import date

def ABS_data_import(url, output_dir="C:/TEMP/", client_timeout=60):
    """Import data from the Australian Bureau of Statistics ABS.Stat API.

    Args:
        url (str): Query URL built at http://stat.data.abs.gov.au/sdmx-json/.
        output_dir (str): Output folder (default is "C:/TEMP/").
        client_timeout (int): Connection timeout length in seconds (default is 60).

    Returns:
        A CSV file into the output_dir folder.

    For more information visit https://www.abs.gov.au/ausstats/abs@.nsf/
    Lookup/1407.0.55.002Main+Features3User+Guide
    """

    # check validity of input arguments

    # check validity of URL
    try:
        dataset_identifier = url.split('/')[5]
        filter_expression = url.split('/')[6]

        params = {}
        if url.split('/')[7].find("?") != -1: # no additional parameters used
            agency_name = url.split('/')[7]
            for i in url.split('/')[7]\
                    [url.split('/')[7].find("?")+1:].split("&"):
                params[i[:i.find("=")]] = i[i.find("=")+1:]
        else:
            agency_name = url.split('/')[7][:url.split('/')[7].find("?")]

        resource_id = "{}/{}/{}".format(
                dataset_identifier,
                filter_expression,
                agency_name
                )

        # there is a problem with dimensionAtObservation=AllDimensions parameter
        # params["dimensionAtObservation"] = "MeasureDimension"
        params.pop("dimensionAtObservation", None)

    except:
        sys.exit("Error: URL is invalid")

    # check validity of output folder
    if not os.path.isdir(output_dir):
        sys.exit("Error: Output folder does not exist")

    # check validity of timeout length
    if type(client_timeout) != int:
        sys.exit("Error: client_timeout value is not an integer")


    # extract data from ABS.Stat

    agency_code = "ABS"

    ABS = Request(agency_code)
    #ABS.client.config["timeout"] = client_timeout
    ABS.timeout = client_timeout
    data_response = ABS.data(resource_id=resource_id, params=params)


    # write extracted data to data frame

    '''
    df = data_response.write(
            data_response.data.series,
            parse_time=False
            ).unstack().reset_index()
    '''
    df = data_response.to_pandas().reset_index()
    
    # rename unnamed column
    df.rename(columns={0:"Value"}, inplace=True)


    # export data frame to CSV file

    df.to_csv("{}/{}_{}.csv".format(
                output_dir,
                dataset_identifier,
                date.today().strftime("%Y%m%d")
                ),
            index=False
            )

    print("Data successfully exported to {}".format(output_dir))


if __name__ == "__main__":
    print("Function ABS_data_import has been imported")
