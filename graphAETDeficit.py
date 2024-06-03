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
              'Temporal': ["1981-2010", "2040-2059 Ensemble GCM"],
              'AETFields': ["AET_Historic", "AET_MidCentury"],
              'DeficitFields': ["Deficit_Historic", "Deficit_MidCentury"]}


# Output Name, OutDir, and Workspace
outName = 'PCM_AETDeficit'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\Graphs'  # Directory Output Location
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

        # Create the dataframe of Veg Types to iterate through
        veg_type = processDic['VegType']
        veg_name = processDic['VegName']

        # Create a DataFrame with the Vegetation Types will be outer loop
        vegTypesDF = pd.DataFrame({'VegType': veg_type, 'VegName': veg_name})

        # Create the DataFrame with the Temporal Periods and Fields to Process
        temporalFields = processDic['Temporal']
        aetFields = processDic['AETFields']
        deficitFields = processDic['DeficitFields']

        # Create a DataFrame defining the temporal information AET and DEficit Fields
        # will be the inter temporal loop - once historic, once futures
        temporalDF = pd.DataFrame({'TemporalFields': temporalFields, 'AETFields': aetFields,
                                   'DeficitFields': deficitFields})


        #########################################################
        # Create Point Graphs by Community
        # #########################################################
        outFun = pointGraphs(pointsDF, vegTypesDF, temporalDF, outDir)
        if outFun.lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function pointGraphs - " + messageTime + " - Failed - Exiting Script")
            exit()

        messageTime = timeFun()
        scriptMsg = f'Successfully completed - pointGraphs.py - {messageTime}'
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        #########################################################
        # Create Vector Graphs by Community Historic to Futures
        #########################################################
        outFun = vectorGraphs(pointsDF, vegTypesDF, temporalDF, outDir)
        if outFun.lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function vectorGraphs - " + messageTime + " - Failed - Exiting Script")
            exit()

        messageTime = timeFun()
        scriptMsg = f'Successfully completed - graphAETDeficit.py - {messageTime}'
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        #########################################################
        # Create Vector Graph Across All Communities
        #########################################################
        outFun = vectorAllCommunities(pointsDF, vegTypesDF, temporalDF, outDir)
        if outFun.lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function vectorAllCommunities - " + messageTime + " - Failed - Exiting Script")
            exit()

        messageTime = timeFun()
        scriptMsg = f'Successfully completed - vectorAllCommunities.py - {messageTime}'
        print(scriptMsg)
        logFile = open(logFileName, "a")
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


def pointGraphs(pointsDF, vegTypesDF, temporalDF, outDir):

    """
    Creates AET/Deficit scatter plots by vegetation type (e.g. Vegetation Type

    :param pointsDF: points dataframe to be processed
    :param vegTypesDF: Dataframe define the Veg Types to be iterated through, this is the out loop
    :param temporalDF: Dataframe defining the Temporal Periods and associated AET and Deficit Fields.  This
    is the inner loop of the nest loop.
    :param outDir: Output directory

    :return: PDFs file with scatter plots of AET/Deficit per Veg Type (i.e. community) at Historical and
    Futures time steps. Exported to the Output Directory.
    """
    try:

        #Prior to graphing remove records with Negative AET or Deficit
        noZeroDF = pointsDF[(pointsDF['AET_Historic'] >= 0) & (pointsDF['AET_MidCentury'] >= 0)]

        # Iterate through the VegTypes
        for index, vegRow in vegTypesDF.iterrows():
            vegTypeLU = vegRow.get("VegType")
            vegNameLU = vegRow.get("VegName")

            #Iterate Through the Temporal Time Steps (i.e. Historic and Futures)
            for index2, timeRow in temporalDF.iterrows():
                timePeriodLU = timeRow.get("TemporalFields")
                aeFieldsLU = timeRow.get("AETFields")
                deficitFieldsLU = timeRow.get("DeficitFields")

                #Subset to only the vegetation of Interest
                noZeroVegTypeDF = noZeroDF[noZeroDF['VegType'] == vegTypeLU]

                #Subset to all other than 'PCM' records
                notPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] != 'PCM']

                #Subset only PCM records
                onlyPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] == 'PCM']

                #Get Count of Records by Source
                countNotPCMDF = notPCMDF.shape[0]
                countOnlyPCMDF = onlyPCMDF.shape[0]

                #Define the Style Mappings - including other shouldn't be need though
                size_mapping = {'PCM': 50, 'GBIF': 10, 'Other': 10}
                color_mapping = {'PCM': '#1f77b4', 'GBIF': '#ff7f0e', 'Other': '#2ca02c'}

                #Define the scatter Plot Size
                plt.figure(figsize=(10, 6))
                # Create the scatter plot with the GBIF (high number of points)
                sns.scatterplot(data=notPCMDF, x=deficitFieldsLU, y=aeFieldsLU, hue='Source', size='Source',
                                sizes=size_mapping, palette=color_mapping)

                #Overlay the PCM Plots
                sns.scatterplot(data=onlyPCMDF, x='Deficit_Historic', y='AET_Historic', hue='Source', size='Source',
                                sizes=size_mapping, palette=color_mapping)

                plt.xlabel('Avg. Total Annual Deficit (mm)')
                plt.ylabel('Avg. Total Annual AET (mm)')

                titleLU = f'{vegNameLU} - {timePeriodLU}'
                plt.title(titleLU)

                # Show plot
                plt.legend(title='Source')

                #Name for output graph
                outPDF = f'{vegTypeLU}_{timePeriodLU}.pdf'
                #Full Path
                outPath = f'{outDir}\\points\\{outPDF}'

                #Delete File IF Exists
                if os.path.exists(outPath):
                    os.remove(outPath)
                #Make points file if needed
                if os.path.exists(f'{outDir}\\points'):
                    pass
                else:
                    os.makedirs(f'{outDir}\\points')

                #Export Plot
                plt.savefig(outPath, format='pdf')

                #Close Plot
                plt.close()

                messageTime = timeFun()
                scriptMsg = f'Successfully created graphed - {vegTypeLU} - {timePeriodLU} - see - {outPath} - {messageTime}'
                print(scriptMsg)

                logFile = open(logFileName, "a")
                logFile.write(scriptMsg + "\n")
                logFile.close()

        return 'success function'

    except:
        print(f'Failed - pointsGraphs')
        exit()


def vectorGraphs(pointsDF, vegTypesDF, temporalDF, outDir):
    """
    Creates AET/Deficit scatter plots Vector Graphs ( change from Historic to Current) by vegetation type
    (e.g. Vegetation Type)

    :param pointsDF: points dataframe to be processed
    :param vegTypesDF: Dataframe define the Veg Types to be iterated through, this is the out loop
    :param temporalDF: Dataframe defining the Temporal Periods and associated AET and Deficit Fields.  This
    is the inner loop of the nest loop.

    :param outDir: Output directory

    :return: PDFs file with scatter plots with vectors of AET/Deficit change (e.g. Historic to MidCentury) per Veg Type
     (i.e. community). Exported to the Output Directory.
    """
    try:

        #Reset the Index making as a field, will be used in the vector graphing as a unique index allowing for
        #Calculation of change across points
        pointsDF.reset_index(inplace=True)

        #Prior to graphing remove records with Negative AET or Deficit
        noZeroDF = pointsDF[(pointsDF['AET_Historic'] >= 0) & (pointsDF['AET_MidCentury'] >= 0)]

        # Iterate through the VegTypes
        for index, vegRow in vegTypesDF.iterrows():
            vegTypeLU = vegRow.get("VegType")
            vegNameLU = vegRow.get("VegName")

            #Define Fields for Vector Analysis from the Temporal Dataframe (should only be two record 1-Historic,
            # 2-Future

            #Get First row from temporal dataframe, will be the Historic fields
            seriesHist = temporalDF.iloc[0]
            timePeriodHist = seriesHist.get("TemporalFields")
            aetFieldsHist = seriesHist.get("AETFields")
            deficitFieldsHist = seriesHist.get("DeficitFields")

            # Get Second row from temporal dataframe, will be the Future fields
            seriesFut = temporalDF.iloc[1]
            timePeriodFut = seriesFut.get("TemporalFields")
            aetFieldsFut = seriesFut.get("AETFields")
            deficitFieldsFut = seriesFut.get("DeficitFields")

            #Subset to only the vegetation of Interest
            noZeroVegTypeDF = noZeroDF[noZeroDF['VegType'] == vegTypeLU]

            #Subset to all other than 'PCM' records
            notPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] != 'PCM']

            #Subset only PCM records
            onlyPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] == 'PCM']

            # #Define the Style Mappings - including other shouldn't be need though
            size_mapping = {'PCM': 50, 'GBIF': 5, 'Other': 10}
            color_mapping = {'PCM': '#000000', 'GBIF': '#d3d3d3', 'Other': '#2ca02c'}

            #Define the scatter Plot Size
            plt.figure(figsize=(10, 6))
            # Create the scatter plot with the GBIF (high number of points)
            sns.scatterplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source', size='Source',
                            sizes=size_mapping, palette=color_mapping)

            # Draw vectors GBIF - iterate through the dataframe sequentially
            for i in range(len(notPCMDF)):

                plt.plot(
                    [notPCMDF[deficitFieldsHist].values[i], notPCMDF[deficitFieldsFut].values[i]],
                    [notPCMDF[aetFieldsHist].values[i], notPCMDF[aetFieldsFut].values[i]],
                    color='#d3d3d3',
                    lw=0.5
                )

                # Add arrow
                plt.annotate(
                    '',
                    xy=(notPCMDF[deficitFieldsFut].values[i], notPCMDF[aetFieldsFut].values[i]),
                    xytext=(notPCMDF[deficitFieldsHist].values[i], notPCMDF[aetFieldsHist].values[i]),
                    arrowprops=dict(arrowstyle="->", color='#d3d3d3', lw=0.5)
                )

            #######################
            #Overlay the PCM Plots

            # Create the scatter plot PCM only plots
            sns.scatterplot(data=onlyPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source', size='Source',
                            sizes=size_mapping, palette=color_mapping)

            # Draw vectors GBIF - iterate through the dataframe sequentially
            for i in range(len(onlyPCMDF)):
                plt.plot(
                    [onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[deficitFieldsFut].values[i]],
                    [onlyPCMDF[aetFieldsHist].values[i], onlyPCMDF[aetFieldsFut].values[i]],
                    color='#000000',
                    lw=1
                )

                # Add arrow
                plt.annotate(
                    '',
                    xy=(onlyPCMDF[deficitFieldsFut].values[i], onlyPCMDF[aetFieldsFut].values[i]),
                    xytext=(onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[aetFieldsHist].values[i]),
                    arrowprops=dict(arrowstyle="->", color='#000000', lw=1)
                )

            # Add 1:1 dashed line
            #Get max value in not PCMDF Dataframe, should get the hightest value in the graph in nearly all cases
            columns_to_include = [deficitFieldsHist, deficitFieldsHist, aetFieldsFut, aetFieldsHist]
            max_val = notPCMDF[columns_to_include].max().max()
            plt.plot([0, max_val], [0, max_val], linestyle='--', color='black')


            plt.xlabel('Avg. Total Annual Deficit (mm)')
            plt.ylabel('Avg. Total Annual AET (mm)')

            titleLU = f'{vegNameLU} - Change from {timePeriodHist} to {timePeriodFut}'
            plt.title(titleLU)

            # Show plot
            plt.legend(title='Source')

            #Name for output graph
            outPDF = f'{vegNameLU}_{timePeriodHist}_{timePeriodFut}.pdf'
            #Full Path
            outPath = f'{outDir}\\vector\\{outPDF}'

            #Delete File IF Exists
            if os.path.exists(outPath):
                os.remove(outPath)
            #Make points file if needed
            if os.path.exists(f'{outDir}\\vector'):
                pass
            else:
                os.makedirs(f'{outDir}\\vector')

            #Export Plot
            plt.savefig(outPath, format='pdf')

            #Close Plot
            plt.close()

            messageTime = timeFun()
            scriptMsg = f'Successfully created Vector graph - {vegTypeLU} - see - {outPath} - {messageTime}'
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        return 'success function'

    except:
        print(f'Failed - vertorGraphs')
        exit()
def vectorAllCommunities(pointsDF, vegTypesDF, temporalDF, outDir):
    """
    Creates AET/Deficit scatter plots Vector Graphs ( change from Historic to Current) for all PCM vegetation.

    :param pointsDF: points dataframe to be processed
    :param vegTypesDF: Dataframe define the Veg Types to be iterated through, this is the out loop
    :param temporalDF: Dataframe defining the Temporal Periods and associated AET and Deficit Fields.

    :param outDir: Output directory

    :return: PDFs file with one graph  vectors of AET/Deficit change (e.g. Historic to MidCentury) across all PCM
    (i.e. community). Exported to the Output Directory.
    """
    try:

        #Reset the Index making as a field, will be used in the vector graphing as a unique index allowing for
        #Calculation of change across points
        pointsDF.reset_index(inplace=True)

        #Prior to graphing remove records with Negative AET or Deficit
        noZeroDF = pointsDF[(pointsDF['AET_Historic'] >= 0) & (pointsDF['AET_MidCentury'] >= 0)]

        # Subset only PCM records
        onlyPCMDF = noZeroDF[noZeroDF['Source'] == 'PCM']

        #Join the vegTypesDF data frame to get the Vegetation Name
        onlyPCMDFwVegName = pd.merge(onlyPCMDF, vegTypesDF, on='VegType', how='inner')


        #Define Fields for Vector Analysis from the Temporal Dataframe (should only be two record 1-Historic,
        # 2-Future
        #Get First row from temporal dataframe, will be the Historic fields
        seriesHist = temporalDF.iloc[0]
        timePeriodHist = seriesHist.get("TemporalFields")
        aetFieldsHist = seriesHist.get("AETFields")
        deficitFieldsHist = seriesHist.get("DeficitFields")

        # Get Second row from temporal dataframe, will be the Future fields
        seriesFut = temporalDF.iloc[1]
        timePeriodFut = seriesFut.get("TemporalFields")
        aetFieldsFut = seriesFut.get("AETFields")
        deficitFieldsFut = seriesFut.get("DeficitFields")

        #Define the Style Mappings - including other shouldn't be need though
        size_mapping = {'California Annual Grassland': 25, 'Blue Oak Woodland': 25, 'Bald Hills Prairie': 25,
                        'Coast Live Oak Woodlands': 25, 'Blue Oak Woodland': 25, 'Douglas Fir Forest': 25,
                        'Coastal Dune Scrub': 25, 'Freshwater Wetlands': 25, 'Redwood Forest': 25,
                        'Coastal Salt Marsh': 25, 'Northern Coastal Scrub': 25, 'Southern Coastal Scrub': 25
                        }

        #Define the scatter Plot Size
        plt.figure(figsize=(10, 6))

        # Create the scatter plot
        palette = sns.color_palette('Paired', onlyPCMDFwVegName['VegName'].nunique())
        sns.scatterplot(data=onlyPCMDFwVegName, x=deficitFieldsHist, y=aetFieldsHist, hue='VegName', sizes=size_mapping, palette=palette)

        # Create a dictionary to map VegType to its color get the palellet/color per vegetation community
        unique_veg_types = onlyPCMDFwVegName['VegName'].unique()
        color_map = {veg_type: palette[i] for i, veg_type in enumerate(unique_veg_types)}

        # Draw vectors - iterate through the dataframe sequentially
        for i in range(len(onlyPCMDFwVegName)):
            #Lookup the VegName value and apply the color ramp with for the Veg Community
            veg_type = onlyPCMDFwVegName['VegName'].values[i]

            plt.plot(
                [onlyPCMDFwVegName[deficitFieldsHist].values[i], onlyPCMDFwVegName[deficitFieldsFut].values[i]],
                [onlyPCMDFwVegName[aetFieldsHist].values[i], onlyPCMDFwVegName[aetFieldsFut].values[i]],
                color=color_map[veg_type],
                lw=1
            )

            # Add arrow
            plt.annotate(
                '',
                xy=(onlyPCMDFwVegName[deficitFieldsFut].values[i], onlyPCMDFwVegName[aetFieldsFut].values[i]),
                xytext=(onlyPCMDFwVegName[deficitFieldsHist].values[i], onlyPCMDFwVegName[aetFieldsHist].values[i]),
                arrowprops=dict(arrowstyle="->", color=color_map[veg_type], lw=1.0)
            )

        plt.xlabel('Avg. Total Annual Deficit (mm)')
        plt.ylabel('Avg. Total Annual AET (mm)')

        titleLU = f'AET and Deficit Change from {timePeriodHist} to {timePeriodFut}'
        plt.title(titleLU)

        # Show plot
        plt.legend(title='Source')

        #Name for output graph
        outPDF = f'All_PCM_Communities_{timePeriodHist}_{timePeriodFut}.pdf'
        #Full Path
        outPath = f'{outDir}\\vector\\{outPDF}'

        #Delete File IF Exists
        if os.path.exists(outPath):
            os.remove(outPath)
        #Make points file if needed
        if os.path.exists(f'{outDir}\\vector'):
            pass
        else:
            os.makedirs(f'{outDir}\\vector')

        #Export Plot
        plt.savefig(outPath, format='pdf')

        #Close Plot
        plt.close()

        messageTime = timeFun()
        scriptMsg = f'Successfully created All PCM Communities Graph - see - {outPath} - {messageTime}'
        print(scriptMsg)

        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return 'success function'

    except:
        print(f'Failed - pointsGraphs')
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