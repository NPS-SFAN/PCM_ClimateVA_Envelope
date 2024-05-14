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
        print(f'Successfully finished processing - {park}')

        inQuery = "Select * FROM tblNAWMADataset"
        outFun = connect_to_AcessDB(inQuery, ISEDDB)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - NAWMADataset" + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            outDfSpecies = outFun[1]
            messageTime = timeFun()
            print(f'Success: connect_to_AcessDB - tranformPINN {messageTime}')



    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - PCM_VegSummaries_ClimateVA.py - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()

#Connect to Access DB and perform defined query - return query in a dataframe
def connect_to_AcessDB(query, inDB):

    try:
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)
        queryDf = pd.read_sql(query, cnxn)
        cnxn.close()

        return "success function", queryDf

def timeFun():
    from datetime import datetime
    b = datetime.now()
    messageTime = b.isoformat()
    return messageTime