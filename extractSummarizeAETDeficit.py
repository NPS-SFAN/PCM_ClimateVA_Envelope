"""
extractSummarizeAETDeficit.py
Script will pull
Input:
    monitoringLoc - Table with defined monitoring locations (e.g. PCM Plots)
    gbifLoc - Extracted Taxon occurrences from GBIF across the CONUS
    waterBalanceData - List of NPS water balance rasters which will be extracted at the 'monitoirngLoc' and 'gbifLoc'
    file point locations

Output:
    1) Table with extracted water balance for the monitoirngLoc and gbifLoc files
    2) Summary graphs of AET by Deficit


Python Environment: PCM_VegClimateVA - Python 3.11

Date Developed - May 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""


import pandas as pd
import sys
import os
import session_info
import traceback
from datetime import datetime

#File (Excel/CSV) with vegtation taxon to be idenfities
monitoringLoc = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\PCM\NAWMACover\PCM_NAWMA_Vegetation_ClimateVA_20240517.xlsx'


# Output Name, OutDir, and Workspace
outName = 'PCM_NAWMA_TopTwo_GBIF'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\GBIF'  # Directory Output Location
workspace = f'{outDir}\\workspace'  # Workspace Output Directory
dateNow = datetime.now().strftime('%Y%m%d')
logFileName = f'{workspace}\\{outName}_{dateNow}.LogFile.txt'  # Name of the .txt script logfile which is saved in the workspace directory



def main():
    try:

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