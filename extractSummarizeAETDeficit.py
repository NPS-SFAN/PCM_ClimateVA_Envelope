"""
extractSummarizeAETDeficit.py
Script will pull
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
Libraries: Geopandas, Raterio, Pandas

Date Developed - May 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""


import pandas as pd
import sys
import os
import session_info
import traceback
from datetime import datetime
#Packages for GIS point extract
import rasterio
from rasterio.transform import from_origin
from rasterio.enums import Resampling

#Excel file with the monitoring Locations
monitoringLoc = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\PCM\NAWMACover\PCM_Plot_Locations_All_wNPSWB_GCS.xlsx'
#Monitoring Locations Dictionary with IDField, Latitude, Longitude and Vegation Type field definitions.
monitoringLocDic = {'Source': 'PCM', 'IDField': 'Name', 'Latitude': 'Latitude', 'Longitude': 'Longitude', 'VegType': 'SiteType'}

#Excel file with the GBIF Locations
gbifLoc = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\GBIF\PCM_NAWMA_TopTwo_GBIF_Occurrences_20240521.csv'
#Monitoring Locations Dictionary with IDField, Latitude, Longitude and Vegation Type field definitions.
gbifLocDic = {'Source': 'GBIF', 'IDField': 'key', 'Latitude': 'decimalLatitude', 'Longitude': 'decimalLongitude',
              'VegType': 'VegCode'}

#List or Dictionary of WB Rasters to process
wbDataDic = {'Variable': ["AET", "AET", "Deficit", "Deficit"],
             'Temporal': ["Historic", "MidCentury", "Historic",
                          "MidCentury"],
             'Path': [r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\WBData\Historic\V_1_5_annual_gridmet_historical_AET_1981_2010_annual_means_cropped_units_mm_GCS.tif',
                      r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\WBData\Futures\ensemble_2040_2069_annual_rcp85_AET_units_mm_GCS.tif',
                      r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\WBData\Historic\V_1_5_annual_gridmet_historical_Deficit_1981_2010_annual_means_cropped_units_mm_GCS.tif',
                      r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\WBData\Futures\ensemble_2040_2069_annual_rcp85_Deficit_units_mm_GCS.tif']}


# Output Name, OutDir, and Workspace
outName = 'PCM_AETDeficit'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit'  # Directory Output Location
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
        outFun = extractWB(outPointsDF)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function extractWB - " + messageTime + " - Failed - Exiting Script")
            exit()

        outPointsWBDF = outFun[1]





        messageTime = timeFun()
        scriptMsg = f'Successfully  - {outPathName} - {messageTime}'
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - extractSummarizeAETDeficit.py - " + messageTime
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

    :return: outPointsDF: data frame with the monitoring and GBIF files appends, with a field subset and field rename.

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
        print (scriptMsg)

        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return 'success function', outPointsDF

    except:
        print(f'Failed - compilePointFiles')
        exit()


def extractWB(outPointsDF, rasterDataDic):
    """
    For the point lat/lon points in the 'outPointsDF' dataframe extracts values for the defined rasters in the
    dictionary - rasterDataDic (i.e. NPS water balance).


    :param outPointsDF: Dataframe with points defining where to extract raster values.
    :param rasterDataDic: Dictionary defining Raster to be processed, include Metdata and Raster Paths.

    :return: outPointsWBDF: data frame with the extracted raster data for the pass points in 'outPointsDF'.

    """
    try:





        messageTime = timeFun()
        scriptMsg = f'Successfully extracted water balance data - {messageTime}'
        print(scriptMsg)

        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return 'success function', outPointsDF

    except:
        print(f'Failed - compilePointFiles')
        exit()



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