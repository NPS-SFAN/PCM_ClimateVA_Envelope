"""
extractAETDeficit.py
Script extracts point values for the defined point locations (e.g. Monitoring Sites, GBIF occurrences, etc.) and
for the defined rasters (e.g. NPS Water Balance Data). Processing assumes spatial coordinates in the point locations
and rasters are in the same projection (e.g. GCS WGS 84).

Raster point extraction is accomplished using the Rasterio (https://rasterio.readthedocs.io/en/stable/) package.

Input:
    monitoringLoc - Table with defined monitoring locations (e.g. PCM Plots)
    gbifLoc - Extracted Taxon occurrences from GBIF across the CONUS
    waterBalanceData - List of NPS water balance rasters which will be extracted at the 'monitoirngLoc' and 'gbifLoc'
    file point locations


    PCM VA - Post projection of the NPS WB Rasters to GCS WGS 84 - 157 PCM sites coincident with NPS Water Balance data.

Output:
    1) Table with extracted water balance for the monitoirngLoc and gbifLoc files
    2) Summary graphs of AET by Deficit


Python Environment: PCM_VegClimateVA - Python 3.11
Libraries: Geopandas, Rasterio, Pandas, Numpy

Date Developed - May 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""


import pandas as pd
import numpy as np
import sys
import os
import session_info
import traceback
from datetime import datetime
#Packages for GIS point extract
import rasterio

#Excel file with the monitoring Locations
monitoringLoc = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\PCM\NAWMACover\PCM_Plot_Locations_All_wNPSWB_GCS.xlsx'
#Monitoring Locations Dictionary with IDField, Latitude, Longitude and Vegation Type field definitions.
monitoringLocDic = {'Source': 'PCM', 'IDField': 'Name', 'Latitude': 'Latitude', 'Longitude': 'Longitude', 'VegType': 'SiteType'}

#Excel file with the GBIF Locations
gbifLoc = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\GBIF\ReferenceTaxon\PCM_Reference_GBIF_Occurrences_20240607.csv'
#Monitoring Locations Dictionary with IDField, Latitude, Longitude and Vegation Type field definitions.
gbifLocDic = {'Source': 'GBIF', 'IDField': 'key', 'Latitude': 'decimalLatitude', 'Longitude': 'decimalLongitude',
              'VegType': 'VegCode', 'Taxon': 'scientificNameLookup'}

#List or Dictionary of WB Rasters to process
rasterDataDic = {'Variable': ["AET", "AET", "Deficit", "Deficit"],
             'Temporal': ["Historic", "MidCentury", "Historic",
                          "MidCentury"],
             'Path': [r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\WBData\Historic\V_1_5_annual_gridmet_historical_AET_1981_2010_annual_means_cropped_units_mm_GCS.tif',
                      r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\WBData\Futures\ensemble_2040_2069_annual_rcp85_AET_units_mm_GCS.tif',
                      r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\WBData\Historic\V_1_5_annual_gridmet_historical_Deficit_1981_2010_annual_means_cropped_units_mm_GCS.tif',
                      r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\WBData\Futures\ensemble_2040_2069_annual_rcp85_Deficit_units_mm_GCS.tif']}


# Output Name, OutDir, and Workspace
outName = 'PCM_AETDeficit_Reference'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\ReferenceTaxon'  # Directory Output Location
workspace = f'{outDir}\\workspace'  # Workspace Output Directory
dateNow = datetime.now().strftime('%Y%m%d')
logFileName = f'{workspace}\\{outName}_{dateNow}.LogFile.txt'  # Name of the .txt script logfile which is saved in the workspace directory


def main():
    try:

        session_info.show()

        #########################################################
        #Import and Compile the Point Tables (Monitoring Loc and GBIF)
        #########################################################
        outFun = compilePointFiles(monitoringLoc, monitoringLocDic, gbifLoc, gbifLocDic)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function compilePointFiles - " + messageTime + " - Failed - Exiting Script")
            exit()

        outPointsDF = outFun[1]

        #########################################################
        # Import and Compile the Point Tables (Monitoring Loc and GBIF)
        #########################################################
        outFun = extractWBP(outPointsDF, rasterDataDic)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function extractWBP - " + messageTime + " - Failed - Exiting Script")
            exit()
        #Output Dataframe with extracted Raster
        outPointsWBDF = outFun[1]

        # Define Path to Output Summary Table
        outPath = f'{outDir}\\{outName}_{dateNow}.csv'

        outPointsWBDF.to_csv(outPath, index=False)

        messageTime = timeFun()
        scriptMsg = f'Exported Extracted Points Table to - {outPath} - {messageTime}'
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        scriptMsg = f'Successfully completed - extractAETDeficit.py - {messageTime}'
        print(scriptMsg)
        logFile.write(scriptMsg + "\n")
        logFile.close()

    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - extractAETDeficit.py - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()
        traceback.print_exc(file=sys.stdout)

    finally:
        exit()


def compilePointFiles(monitoringLoc, monitoringLocDic, gbifLoc, gbifLocDic):
    """
    Function imports and compiles the 'monitoring location' and 'gbif location' tables.  Appends to the two input tables
    (i.e. monitoringLoc and gbifLoc) and applies as field subset and subsequent field rename using the pass dictionaries
    (i.e. monitoringLocDic and gbifLocDic).

    :param monitoringLoc: Monitoring Locations Table.
    :param monitoringLocDic: Monitoring Locations Table field dictionary
    :param gbifLoc: GBIF Locations Tables
    :param gbifLocDic: GBIF Locations Tables field dictionary

    :return: outPointsDF: data frame with the monitoring and GBIF files appended, with a field subset and field rename.

    """
    try:

        ########################
        # Read in the Monitoring Loc and GBIF Tables
        ########################

        inFormat = os.path.splitext(monitoringLoc)[1]
        if inFormat == '.xlsx':
            inMonLocDF = pd.read_excel(monitoringLoc)
        elif inFormat == '.csv':
            inMonLocDF = pd.read_csv(monitoringLoc)

        inFormat = os.path.splitext(gbifLoc)[1]
        if inFormat == '.xlsx':
            inGbifDF = pd.read_excel(gbifLoc)
        elif inFormat == '.csv':
            inGbifDF = pd.read_csv(gbifLoc)

        ##################
        ###Process Monitoring Locations DF
        ##################

        #Append Data Frame List
        appendDFList = []

        #Locations Field List to subset
        fieldList = list(monitoringLocDic.values())

        #Extract the Source Key value
        sourceVal = monitoringLocDic['Source']

        #Remove Source Key Value Field
        fieldList.remove(sourceVal)

        #Subset to desired fields
        inMonLocDFSub = inMonLocDF[fieldList]

        #Add the Source Field and define with the 'sourceVal' assuming Source Value is always the first value
        inMonLocDFSub.insert(0, 'Source', sourceVal)

        #Define the desired common fields names to be used across the two dataframes
        newFieldList = list(monitoringLocDic.keys())

        #Rename subset fields:
        inMonLocDFSub.columns = newFieldList

        appendDFList.append(inMonLocDFSub)

        print(f'Successfully preprocessed Monitoring Locations Table')

        ##################
        ###Process GBIF DF
        ##################

        # Locations Field List to subset
        fieldList = list(gbifLocDic.values())

        # Extract the Source Key value
        sourceVal = gbifLocDic['Source']

        # Remove Source Key Value Field
        fieldList.remove(sourceVal)

        # Subset to desired fields
        inGBIFDFSub = inGbifDF[fieldList]

        # Add the Source Field and define with the 'sourceVal' assuming Source Value is always the first value
        inGBIFDFSub.insert(0, 'Source', sourceVal)

        # Define the desired common fields names to be used across the two dataframes
        newFieldList = list(gbifLocDic.keys())

        # Rename subset fields:
        inGBIFDFSub.columns = newFieldList

        appendDFList.append(inGBIFDFSub)
        print(f'Successfully preprocessed GBIF Locations')

        #Concate All dataframes in list to one occurrence dataframe
        outPointsDF = pd.concat(appendDFList, ignore_index=True)

        messageTime = timeFun()
        scriptMsg = f'Successfully preprocessed and merged Locations and GBIF tables - {messageTime}'
        print(scriptMsg)

        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return 'success function', outPointsDF

    except:
        print(f'Failed - compilePointFiles')
        exit()


def extractWBP(pointsDF, rasterDataDic):
    """
    For the point lat/lon points in the 'outPointsDF' dataframe extracts values for the defined rasters in the
    dictionary - rasterDataDic (i.e. NPS water balance). Parallel Processing via ThreadPoolExecutor.
    is being used to increase processing speed

    :param pointsDF: Dataframe with points defining where to extract raster values.
    :param rasterDataDic: Dictionary defining Raster to be processed, include Metdata and Raster Paths.

    :return: outPointsWBDF: data frame with the extracted raster data for the pass points in 'outPointsDF'.

    """
    try:

        from concurrent.futures import ThreadPoolExecutor, as_completed
        #Convert the Raster Dictionary to a Dataframe - to iterate through
        rasterDF = pd.DataFrame.from_dict(rasterDataDic, orient='columns')

        # Extract the latitude and longitude lists
        lats = pointsDF['Latitude'].tolist()
        lons = pointsDF['Longitude'].tolist()


        #Iterate through the Rasters
        for index, row in rasterDF.iterrows():
            variableLU = row.get("Variable")
            temporalLU = row.get("Temporal")
            pathLU = row.get("Path")

            fieldName = f'{variableLU}_{temporalLU}'

            'Check that raster path exists'
            if os.path.exists(pathLU) != True:
                messageTime = timeFun()
                msgScript = (f'Warning Raster Path - {pathLu} - doesnt exist - exiting script')
                print(scriptMsg)
                logFile = open(logFileName, "a")
                logFile.write(scriptMsg + "\n")
                logFile.close()
                sys.exit()

            #Open the raster file
            raster = rasterio.open(pathLU)

            # Read the raster data
            raster_data = raster.read(1)

            # Use ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(get_raster_value, lat, lon, raster, raster_data): i for i, (lat, lon) in
                           enumerate(zip(lats, lons))}
                results = [None] * len(futures)  # Initialize a list to store results in the correct order
                for future in as_completed(futures):
                    index = futures[future]
                    results[index] = future.result()

            #Export Points Back to the Dataframe
            pointsDF[fieldName] = results

            messageTime = timeFun()
            scriptMsg = f'Successfully extracted water balance data for {fieldName} - {messageTime}'
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        outPointsWBDF = pointsDF
        return 'success function', outPointsWBDF

    except:
        print(f'Failed - extractWBP')
        exit()






def get_raster_value(lat, lon, raster, raster_data):
# Function to extract raster value at a given point
    try:
        # Convert latitude and longitude to the raster's coordinate system
        row, col = raster.index(lon, lat)

        # Check if the indices are within the raster bounds
        if (0 <= row < raster.height) and (0 <= col < raster.width):
            return raster_data[row, col]
        else:
            return np.nan  # Return NaN if the point is out of bounds
    except IndexError:
        return np.nan  # Handle case where raster.index() fails

def timeFun():
    from datetime import datetime
    b = datetime.now()
    messageTime = b.isoformat()
    return messageTime

if __name__ == '__main__':

    #################################
    # Checking for Out Directories and Log File
    ##################################
    if os.path.exists(outDir):
        pass
    else:
        os.makedirs(outDir)

    if os.path.exists(workspace):
        pass
    else:
        os.makedirs(workspace)

    # Check if logFile exists
    if os.path.exists(logFileName):
        pass
    else:
        logFile = open(logFileName, "w")  # Creating index file if it doesn't exist
        logFile.close()

    # Run Main Code Bloc
    main()
