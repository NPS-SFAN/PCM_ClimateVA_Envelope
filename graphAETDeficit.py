"""
graphAETDeficitPCM.py

For extracted AET, and Deficit data creates summary graphs
Input:
   Point file with extracted Monitoring Locations and Water Balance Data
Output:
    1) PDF Figures of AET by Deficit


Python Environment: PCM_VegClimateVA - Python 3.11
Libraries:

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


#Excel file with the monitoring Locations
inPointsWB = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\PCM_AETDeficit_20240530.csv'


# Output Name, OutDir, and Workspace
outName = 'PCM_AETDeficit'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit'  # Directory Output Location
workspace = f'{outDir}\\workspace'  # Workspace Output Directory
dateNow = datetime.now().strftime('%Y%m%d')
logFileName = f'{workspace}\\{outName}_{dateNow}.LogFile.txt'  # Name of the .txt script logfile which is saved in the workspace directory


def main():
    try:
        #Iterate Through the Vegtatation Types - Graphs with Historic, Futures, and Hisoric and Futures
        #Consider Vector as well mimic Mike's PLOS on AET/Deficit Vectors.
        




        scriptMsg = f'Successfully completed - graphAETDeficit.py - {messageTime}'
        print(scriptMsg)
        logFile.write(scriptMsg + "\n")
        logFile.close()

    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - graphAETDeficit.py - " + messageTime
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