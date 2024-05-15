"""
PCM_VegSummaries_ClimateVA.py
Script performs summary of the PCM dataset files as partof the SFAN PCM Climate Change Vulnerability Assessment.

Python Environment: PCM_VegClimateVA - Python 3.11

Date Developed - March 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""

#Import Libraries
import pandas as pd
import pyodbc
import sys
import os
import session_info
import traceback
from datetime import datetime

#PCM Database
inDB = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\PlantCommunities\Data\Database\SFAN_PlantCommunities_BE_20240306.accdb'

#Output Name, OutDir, and Workspace
outNameLog = 'PCM_Vegetation_ClimateVA'  #Output name for logfile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\PCM'  #Directory Output Location
workspace = f'{outDir}\\workspace'  #Workspace Output Directory
dateNow = datetime.now().strftime('%Y%m%d')
logFileName = f'{workspace}\\{outNameLog}_{dateNow}.LogFile.txt'  #Name of the .txt script logfile which is saved in the workspace directory

def main():
    try:
        session_info.show()

        inQuery = "Select * FROM tblNAWMADataset"
        outFun = connect_to_AcessDB(inQuery, inDB)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - NAWMADataset" + messageTime + " - Failed - Exiting Script")
            exit()

        messageTime = timeFun()
        print(f'Success: connect_to_AcessDB - tranformPINN {messageTime}')
        outDfNAWMA = outFun[1]

        #####
        #Calculate NAWMA Site Percentage Cover By Species By Location Event - Pulling from tblNAWMADataset
        #####
        outFun = NAWMA_CoverByEvent(outDfNAWMA)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - nawma_CoverByEvent" + messageTime + " - Failed - Exiting Script")
            exit()



    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - PCM_VegSummaries_ClimateVA.py - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()
    finally:
        exit()

def NAWMA_CoverByEvent(inDF):
    """
    Create the NAWMA Site Percentage Cover By Species By Location Event Summary - Pulling from tblNAWMADataset.
    Recreating the qrpt_NAWMA_SpeciesPercentCover summary in the PCM Front End but summarzing at the Event rather then
    plot scale.

    Suggested items to define for this class below:

    :param inDF: dataframe with the tblNAWMADataset dataset files

    :return: outSummaryDF: Data Frame with the Summary output
    """
    try:
        #Remove NAWMA Plots - only retaining A, B, C subplots which have 50 hits per
        nawmaDFSet =

        result = inDF.groupBy(['EventID',])


        return outSummaryDF

    except:
        print(f'Failed - NAWMA_CoverByEvent')
        exit()




#Connect to Access DB and perform defined query - return query in a dataframe
def connect_to_AcessDB(query, inDB):

    try:
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)
        queryDf = pd.read_sql(query, cnxn)
        cnxn.close()

        return "success function", queryDf
    except:
        print(f'Failed - connect_to_AccessDB')
        exit()

def timeFun():
    try:
        b = datetime.now()
        messageTime = b.isoformat()
        return messageTime
    except:
        print(f'Failed - timeFun')
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

    #Run Main Code Bloc
    main()