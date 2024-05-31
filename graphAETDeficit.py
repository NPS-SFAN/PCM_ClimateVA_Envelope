"""
graphAETDeficitPCM.py

For extracted AET, and Deficit data creates summary graphs
By Vegetation Type (Iterate):
1) Graphs AET/Deficit Points: Historic, Futures, and Historic and Futures
2) Graphs AET/Deficit vectors: Historic, Futures, and Historic and Futures

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
import sys
import os
import traceback
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

#Excel file with the monitoring Locations
inPointsWB = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\PCM_AETDeficit_20240530.csv'

#Define the dictionary with the Vegetation Type (i.e. Codes), Vegation Names, AET Fields, and Deficit fields to process
processDic = {'VegType': ["ANGR", "BLUO", "CHRT", "CLOW", "DEPR", "DGLF", "DUNE", "FRSH", "REDW", "SALT", "SCRB",
                          "SSCR"],
              'VegName': ["California Annual Grassland", "Blue Oak Woodland", "Bald Hills Prairie",
                          "Coast Live Oak Woodlands", "Coastal Terrace Prairie", "Douglas Fir Forest",
                          "Coastal Dune Scrub", "Freshwater Wetlands", "Redwood Forest", "Coastal Salt Marsh",
                          "Northern Coastal Scrub", "Southern Coastal Scrub"],
              'Temporal': ["Historic (1981-2010)", "Mid Century (2040-2059)"],
              'AETFields': ["AET_Historic", "AET_MidCentury"],
              'DeficitFields': ["Deficit_Historic", "Deficit_MidCentury"]}


# Output Name, OutDir, and Workspace
outName = 'PCM_AETDeficit'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit'  # Directory Output Location
workspace = f'{outDir}\\workspace'  # Workspace Output Directory
dateNow = datetime.now().strftime('%Y%m%d')
logFileName = f'{workspace}\\{outName}_{dateNow}.LogFile.txt'  # Name of the .txt script logfile which is saved in the workspace directory


def main():
    try:

        inFormat = os.path.splitext(inPointsWB)[1]
        if inFormat == '.xlsx':
            pointsDF = pd.read_excel(inPointsWB)
        elif inFormat == '.csv':
            pointsDF = pd.read_csv(inPointsWB)

        #Convert the Process Dictionary to a Dataframe


        #########################################################
        # Import and Compile the Point Tables (Monitoring Loc and GBIF)
        #########################################################
        outFun = pointGraphs(pointsDF, processDic, outDir)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function pointGraphs - " + messageTime + " - Failed - Exiting Script")
            exit()

        outPointsDF = outFun[1]





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


def pointGraphs(pointsDF, processDic, outDir):

    """
    Creates AET/Deficit scatter plots by vegetation type (e.g. Vegetation Type

    :param pointsDF: points dataframe to be processed
    :param processDic: Dictionary defining the Veg Types, Names and AET and Deficit Fields
    :param outDir: Output directory

    :return: PDFs file with scatter plots of AET/Deficit per Veg Type (i.e. community) at Historica and
    Futures time steps.
    """
    try:

        # Create the dataframe of Veg Types to iterate through
        veg_type = processDic['VegType']
        veg_name = processDic['VegName']

        # Create a DataFrame with the Vegetation Types will be outer loop
        vegTypesDF = pd.DataFrame({'VegType': veg_type, 'VegName': veg_name})

        # Create the dataframe of Veg Types to iterate through
        temporalFields = processDic['Temporal']
        aetFields = processDic['AETFields']
        deficitFields = processDic['DeficitFields']

        # Create a DataFrame defining the temporal information AET and DEficit Fields
        # will be the inter temporal loop - once historic, once futures
        temporalDF = pd.DataFrame({'TemporalFields': temporalFields, 'AETFields': aetFields,
                                   'DeficitFields': deficitFields})


        ###STOPPED Here 5/30 - PM





        #Remove Records with Negative AET or Deficit
        filteredDF = pointsDF[(pointsDF['AET_Historic'] >= 0) & (pointsDF['AET_MidCentury'] >= 0)]

        #Subset to all other than 'PCM' records
        notPCMDF = filteredDF[filteredDF['Source'] != 'PCM']

        #Subset only PCM records
        onlyPCMDF = filteredDF[filteredDF['Source'] == 'PCM']

        #Define the Style Mappings
        #style_mapping = {'PCM': 'o', 'GBIF': 's', 'Other': 'X'}
        size_mapping = {'PCM': 50, 'GBIF': 10, 'Other': 10}
        color_mapping = {'PCM': '#1f77b4', 'GBIF': '#ff7f0e', 'Other': '#2ca02c'}

        #Define the Color Palette
        #sns.color_palette("flare", as_cmap=True)

        #Define the scatter Plot Size
        plt.figure(figsize=(10, 6))
        # Create the scatter plot with the GBIF (high number of points)
        sns.scatterplot(data=notPCMDF, x='Deficit_Historic', y='AET_Historic', hue='Source', size='Source',
                        sizes=size_mapping, palette=color_mapping)

        #Overlay the PCM Plots
        sns.scatterplot(data=onlyPCMDF, x='Deficit_Historic', y='AET_Historic', hue='Source', size='Source',
                        sizes=size_mapping, palette=color_mapping)

        plt.xlabel('Avg. Total Annual AET (mm)')
        plt.ylabel('Avg. Total Annual Deficit (mm)')
        plt.title('Scatter Plot of AET Historic vs AET MidCentury Grouped by Source')

        # Show plot
        plt.legend(title='Source')

        outPDF = "TBD"
        outPath = f'{outDir}\\{outName}.pdf'
        #Export Plot
        plt.savefig('scatter_plot.pdf', format='pdf')

        #Close Plot
        plt.close()


        messageTime = timeFun()
        scriptMsg = f'Successfully preprocessed and merged Locations and GBIF tables - {messageTime}'
        print(scriptMsg)

        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return 'success function', outPointsDF

    except:
        print(f'Failed - pointsGraphs')
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