"""
graphAETDeficitPCM.py

For extracted AET, and Deficit data creates summary graphs
By Vegetation Type (Iterate):
1) Graphs AET/Deficit Points: Historic, Futures, and Historic and Futures, by PCM community. Outfolder: 'points'
2) Graphs AET/Deficit Vectors change from historic (i.e. 1981-2010) to futures (i.e. Mid Century 2040-2069) PCM and GBIF
by PCM community. Out Folder 'vector'
3) Graphs of Vector change from historic (i.e. 1981-2010) to futures (i.e. Mid Century 2040-2069) PCM only all
communities on one graph. Out Folder 'vector'
4) Graphs AET/Deficit Vectors change from historic (i.e. 1981-2010) to futures (i.e. Mid Century 2040-2069) PCM only,
with GBIF Historic Points plots, by PCM Community. Out Folder 'VectorPCM_GBIFHistoricPts'

5) Graphs AET/Deficit Vectors change from historic (i.e. 1981-2010) to futures (i.e. Mid Century 2040-2069) PCM only,
with GBIF Historic Points plots and legend symbology with Taxon Scientific Name, Exports are by PCM Community.
Out Folder 'VectorPCM_GBIFHistoric_wTaxon'

6) Graphs AET/Deficit Vectors change from historic (i.e. 1981-2010) to futures (i.e. Mid Century 2040-2069) PCM only,
with GBIF Historic Points plots and legend symbology with Taxon Scientific Name. Additionally a Kernel Density Estimate
(KDE) Function Percentile Values for the GBIF historic data are plots.

The KDE is the smoothed estimate of the data distribution of the points in data space
(ie. AET and Deficit). A contour at a higher percentile level encloses a smaller area with a higher density of
points. This is because it represents a higher threshold of density. Exports are by PCM Community.
Out Folder 'VectorPCM_GBIFHistoric_wPercentile'

7) Graphs with AET/Deficit scatter plots Vector Graphs (change from Historic to Current) by vegetation type for PCM
    plots and graphs points for GBIF historic data.  Graph symbology includes GBIF Taxon (i.e. Taxon by Veg Type).
    Vectors include Ensemble Mean, Warm Wet and Hot Dry. Output figure also includes a map of the points locations used
    in the bioclimatic analysis.

Input:
   Point file with extracted Monitoring Locations and other points of interest (e.g. GBIF occurrences) with
   NPS Water Balance Data extracted
Output:
    1) See descriptions above.


Python Environment: PCM_VegClimateVA - Python 3.11
Libraries:

Date Developed - June/July 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""

import pandas as pd
import sys
import os
import traceback
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import seaborn as sns
import numpy as np
from scipy.stats import gaussian_kde
import geopandas as gpd
import contextily as ctx

# Excel file with the Monitoring Location and GBIF Obserations and extracted AET and Deficit values
inPointsWB = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\ReferenceTaxon\PCM_AETDeficit_Reference_20240916.csv'

# Define the dictionary with the Vegetation Type (i.e. Codes), Vegation Names, AET Fields, and Deficit fields to process
processDic = {'VegType': ["ANGR", "BLUO", "CHRT", "CLOW", "DEPR", "DGLF", "DUNE", "FRSH", "REDW", "SALT", "SCRB",
                          "SSCR"],
              'VegName': ["California Annual Grassland", "Blue Oak Woodland", "Bald Hills Prairie",
                          "Coast Live Oak Woodlands", "Coastal Terrace Prairie", "Douglas Fir Forest",
                          "Coastal Dune Scrub", "Freshwater Wetlands", "Redwood Forest", "Coastal Salt Marsh",
                          "Northern Coastal Scrub", "Southern Coastal Scrub"],
              'Temporal': ["1981-2010", "2040-2069 Ensemble GCM", "2040-2069 Warm Wet", "2040-2069 Hot Dry"],
              'AETFields': ["AET_Historic", "AET_Ensemble_MidCentury", "AET_WW_MidCentury", "AET_HD_MidCentury"],
              'DeficitFields': ["Deficit_Historic", "Deficit_Ensemble_MidCentury", "Deficit_WW_MidCentury",
                                "Deficit_HD_MidCentury"]}

# List of Graphs to Create
# analysisList = ['pointGraphs', 'vectorGraphs', 'vectorAllCommunities', 'vectorPCMPointsGBIFHist',
#                  'vectorPCMPtsGBIFHistPerc', 'vectorPCMPointsGBIFHistwTaxon', 'vectorPCMPointsGBIFHistwTaxonWWHD',
#                  'vectorPCMPointsGBIFHistwTaxonWWHDOne']

analysisList = ['vectorPCMPointsGBIFHistwTaxonWWHDOne']

# Variable defines the size of the output figure - currently only being used in the 'vectorPCMPointsGBIFHistwTaxonWWHD'
# graph, First Column is width, second Height in inches
figSize = [20, 12]  # Use this size for 2x2 outputs

# Variables for the Kernel Density Estimate Percentile Contours
# Percentile Breaks
percentiles = [90]

# Define the colors for each percentile contour line
percentile_colors = {90: 'blue'}

# Output Name, OutDir, and Workspace
outName = 'PCM_AETDeficit'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\AETDeficit\ReferenceTaxon\Graphs'  # Directory Output Location
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

        ########################################################
        # Create Point Graphs by Community
        #########################################################

        if 'pointGraphs' in analysisList:

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
        if 'vectorGraphs' in analysisList:
            outFun = vectorGraphs(pointsDF, vegTypesDF, temporalDF, outDir)
            if outFun.lower() != "success function":
                messageTime = timeFun()
                print("WARNING - Function vectorGraphs - " + messageTime + " - Failed - Exiting Script")
                exit()

            messageTime = timeFun()
            scriptMsg = f'Successfully completed - vectorGraphs - {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        #########################################################
        # Create Vector Graph Across All Communities
        #########################################################
        if 'vectorAllCommunities' in analysisList:
            outFun = vectorAllCommunities(pointsDF, vegTypesDF, temporalDF, outDir)
            if outFun.lower() != "success function":
                messageTime = timeFun()
                print("WARNING - Function vectorAllCommunities - " + messageTime + " - Failed - Exiting Script")
                exit()

            messageTime = timeFun()
            scriptMsg = f'Successfully completed - vectorAllCommunities - {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        #########################################################
        # Create Vector Graphs PCM, Points GBIF Historic
        #########################################################
        if 'vectorPCMPointsGBIFHist' in analysisList:
            outFun = vectorPCMPointsGBIFHist(pointsDF, vegTypesDF, temporalDF, outDir)
            if outFun.lower() != "success function":
                messageTime = timeFun()
                print("WARNING - Function vectorPCMPointsGBIFHist - " + messageTime + " - Failed - Exiting Script")
                exit()

            messageTime = timeFun()
            scriptMsg = f'Successfully completed - vectorPCMPointsGBIFHist - {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        #########################################################
        # Create Vector Graphs PCM, Points GBIF Historic with Kernel Density Estimate Percentile Contours
        #########################################################
        if 'vectorPCMPtsGBIFHistPerc' in analysisList:
            outFun = vectorPCMPtsGBIFHistPerc(pointsDF, vegTypesDF, temporalDF, outDir, percentiles,
                                              percentile_colors)
            if outFun.lower() != "success function":
                messageTime = timeFun()
                print("WARNING - Function vectorPCMPtsGBIFHistPerc - " + messageTime + " - Failed - Exiting Script")
                exit()

            messageTime = timeFun()
            scriptMsg = f'Successfully completed - vectorPCMPtsGBIFHistPerc - {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()


        #########################################################
        # Create Vector Graphs PCM, Points GBIF Historic w Taxon
        #########################################################
        if 'vectorPCMPointsGBIFHistwTaxon' in analysisList:
            outFun = vectorPCMPointsGBIFHistwTaxon(pointsDF, vegTypesDF, temporalDF, outDir)
            if outFun.lower() != "success function":
                messageTime = timeFun()
                print("WARNING - Function vectorPCMPointsGBIFHistwTaxon - " + messageTime + " - Failed - Exiting Script")
                exit()

            messageTime = timeFun()
            scriptMsg = f'Successfully completed - vectorPCMPointsGBIFHistwTaxon - {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        #########################################################
        # Create Vector Graphs PCM, Points GBIF Historic w Taxon, Ensemble, Warm Wet and Hot Dry Vectors and spatial
        # map.  2x2 figure, using the 'figSize' variable to determine the desired output figure size
        #########################################################
        if 'vectorPCMPointsGBIFHistwTaxonWWHD' in analysisList:
            outFun = vectorPCMPointsGBIFHistwTaxonWWHD(pointsDF, vegTypesDF, temporalDF, figSize, outDir)
            if outFun.lower() != "success function":
                messageTime = timeFun()
                print(
                    "WARNING - Function vectorPCMPointsGBIFHistwTaxonWWHD - " + messageTime + " - Failed - Exiting Script")
                exit()

            messageTime = timeFun()
            scriptMsg = f'Successfully completed - vectorPCMPointsGBIFHistwTaxonWWHD - {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        #########################################################
        # Creates AET/Deficit scatter plots Vector Graphs (change from Historic to Current) by vegetation type for PCM
        # plots and graphs points for GBIF historic data in a 1x1 figure (i.e. on figure).
        # Graph symbology includes GBIF Taxon (i.e. Taxon by Veg Type). Vectors include Warm Wet and Hot Dry only.
        #
        # Figure being created for the CCAM Manuscript
        #########################################################
        if 'vectorPCMPointsGBIFHistwTaxonWWHDOne' in analysisList:
            outFun = vectorPCMPointsGBIFHistwTaxonWWHDOne(pointsDF, vegTypesDF, temporalDF, figSize, outDir)
            if outFun.lower() != "success function":
                messageTime = timeFun()
                print(
                    "WARNING - Function vectorPCMPointsGBIFHistwTaxonWWHDOne - " + messageTime + " - Failed - Exiting Script")
                exit()

            messageTime = timeFun()
            scriptMsg = f'Successfully completed - vectorPCMPointsGBIFHistwTaxonWWHDOne - {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        messageTime = timeFun()
        scriptMsg = f'Successfully completed - graphAETDeficit.py - {messageTime}'
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

        # Prior to graphing remove records without data this will be -3.4028235E+38, all data less than 0 was set to 0
        # the 'extractAETDeficit routine
        noZeroDF = pointsDF[pointsDF['AET_Historic'] >= 0]

        #Iterate through the VegTypes
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
                plt.legend()

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

        # Prior to graphing remove records without data this will be -3.4028235E+38, all data less than 0 was set to 0
        # the 'extractAETDeficit routine
        noZeroDF = pointsDF[pointsDF['AET_Historic'] >= 0]

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
            plt.plot([max_val, 0], [0, max_val], linestyle='--', color='black')


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

        # Prior to graphing remove records without data this will be -3.4028235E+38, all data less than 0 was set to 0
        # the 'extractAETDeficit routine
        noZeroDF = pointsDF[pointsDF['AET_Historic'] >= 0]

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
        plt.legend()

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
        print(f'Failed - vectorAllCommunities')
        exit()

def vectorPCMPointsGBIFHist(pointsDF, vegTypesDF, temporalDF, outDir):

    """
    Creates AET/Deficit scatter plots Vector Graphs (change from Historic to Current) by vegetation type for PCM
    plots and graphs points for GBIF historic data.

    :param pointsDF: points dataframe to be processed
    :param vegTypesDF: Dataframe define the Veg Types to be iterated through, this is the out loop
    :param temporalDF: Dataframe defining the Temporal Periods and associated AET and Deficit Fields.  This
    is the inner loop of the nest loop.
    :param outDir: Output directory

    :return: PDFs file with Vector plots AET/Deficit per Veg Type (i.e. community) for PCM plots with Historical
     GBIF points being graphed. Exported to the 'VectorPCM_GBIFHistoricPts' directory.
    """
    try:

        # Prior to graphing remove records without data this will be -3.4028235E+38, all data less than 0 was set to 0
        # the 'extractAETDeficit routine
        noZeroDF = pointsDF[pointsDF['AET_Historic'] >= 0]

        # Iterate through the VegTypes
        for index, vegRow in vegTypesDF.iterrows():
            vegTypeLU = vegRow.get("VegType")
            vegNameLU = vegRow.get("VegName")

            # Define Fields for Vector Analysis from the Temporal Dataframe (should only be two record 1-Historic,
            # 2-Future

            # Get First row from temporal dataframe, will be the Historic fields
            seriesHist = temporalDF.iloc[0]
            timePeriodHist = seriesHist.get("TemporalFields")
            aetFieldsHist = seriesHist.get("AETFields")
            deficitFieldsHist = seriesHist.get("DeficitFields")

            # Get Second row from temporal dataframe, will be the Future fields
            seriesFut = temporalDF.iloc[1]
            timePeriodFut = seriesFut.get("TemporalFields")
            aetFieldsFut = seriesFut.get("AETFields")
            deficitFieldsFut = seriesFut.get("DeficitFields")

            # Subset to only the vegetation of Interest
            noZeroVegTypeDF = noZeroDF[noZeroDF['VegType'] == vegTypeLU]

            # Subset to all other than 'PCM' records
            notPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] != 'PCM']

            # Subset only PCM records
            onlyPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] == 'PCM']

            # #Define the Style Mappings - including other shouldn't be need though
            size_mapping = {'PCM': 50, 'GBIF': 10, 'Other': 10}
            color_mapping = {'PCM': '#000000', 'GBIF': '#ff7f0e', 'Other': '#2ca02c'}

            # Define the scatter Plot Size
            plt.figure(figsize=(10, 6))

            # Create the scatter plot with the GBIF (high number of points)
            sns.scatterplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source', size='Source',
                            sizes=size_mapping, palette=color_mapping)

            #######################
            # Overlay the PCM Plots

            # Create the scatter plot PCM only plots
            scatterPlot = sns.scatterplot(data=onlyPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source', size='Source',
                            sizes=size_mapping, palette=color_mapping)

            new_labels = ['GBIF Historic (1981-2010)', 'PCM Plots']
            handles, labels = scatterPlot.get_legend_handles_labels()
            scatterPlot.legend(handles=handles, labels=new_labels)


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
            # Get max value in not PCMDF Dataframe, should get the hightest value in the graph in nearly all cases
            columns_to_include = [deficitFieldsHist, deficitFieldsHist, aetFieldsFut, aetFieldsHist]
            max_val = notPCMDF[columns_to_include].max().max()
            plt.plot([max_val, 0], [0, max_val], linestyle='--', color='black')

            plt.xlabel('Avg. Total Annual Deficit (mm)')
            plt.ylabel('Avg. Total Annual AET (mm)')

            titleLU = f'{vegNameLU} - PCM change from {timePeriodHist} to {timePeriodFut}'
            plt.title(titleLU)

            # Name for output graph
            outPDF = f'{vegNameLU}_PCMVector_{timePeriodHist}_{timePeriodFut}_GBIF_Historic.pdf'

            #OutFolder
            outFolder = 'VectorPCM_GBIFHistoricPts'

            # Full Path
            outPath = f'{outDir}\\{outFolder}\\{outPDF}'

            # Delete File IF Exists
            if os.path.exists(outPath):
                os.remove(outPath)
            # Make points file if needed
            if os.path.exists(f'{outDir}\\{outFolder}'):
                pass
            else:
                os.makedirs(f'{outDir}\\{outFolder}')

            # Export Plot
            plt.savefig(outPath, format='pdf')

            # Close Plot
            plt.close()

            messageTime = timeFun()
            scriptMsg = f'Successfully created Vector graph - {vegTypeLU} for PCM, points GBIF Historic - see - {outPath} - {messageTime}'
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        return 'success function'

    except:
        print(f'Failed - vectorPCMPointsGBIFHist')
        exit()
def vectorPCMPointsGBIFHistwTaxon(pointsDF, vegTypesDF, temporalDF, outDir):

    """
    Creates AET/Deficit scatter plots Vector Graphs (change from Historic to Current) by vegetation type for PCM
    plots and graphs points for GBIF historic data.  Graph symbology includes GBIF Taxon (i.e. Taxon by Veg Type).

    :param pointsDF: points dataframe to be processed
    :param vegTypesDF: Dataframe define the Veg Types to be iterated through, this is the out loop
    :param temporalDF: Dataframe defining the Temporal Periods and associated AET and Deficit Fields.  This
    is the inner loop of the nest loop.
    :param outDir: Output directory

    :return: PDFs file with Vector plots AET/Deficit per Veg Type (i.e. community) for PCM plots with Historical
     GBIF points being graphed. Exported to the 'VectorPCM_GBIFHistoricPts' directory.
    """
    try:

        # Prior to graphing remove records without data this will be -3.4028235E+38, all data less than 0 was set to 0
        # the 'extractAETDeficit routine
        noZeroDF = pointsDF[pointsDF['AET_Historic'] >= 0]

        # Iterate through the VegTypes
        for index, vegRow in vegTypesDF.iterrows():
            vegTypeLU = vegRow.get("VegType")
            vegNameLU = vegRow.get("VegName")

            # Define Fields for Vector Analysis from the Temporal Dataframe (should only be two record 1-Historic,
            # 2-Future

            # Get First row from temporal dataframe, will be the Historic fields
            seriesHist = temporalDF.iloc[0]
            timePeriodHist = seriesHist.get("TemporalFields")
            aetFieldsHist = seriesHist.get("AETFields")
            deficitFieldsHist = seriesHist.get("DeficitFields")

            # Get Second row from temporal dataframe, will be the Future fields
            seriesFut = temporalDF.iloc[1]
            timePeriodFut = seriesFut.get("TemporalFields")
            aetFieldsFut = seriesFut.get("AETFields")
            deficitFieldsFut = seriesFut.get("DeficitFields")

            # Subset to only the vegetation of Interest
            noZeroVegTypeDF = noZeroDF[noZeroDF['VegType'] == vegTypeLU]

            # Subset to all other than 'PCM' records
            notPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] != 'PCM']

            # Subset only PCM records
            onlyPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] == 'PCM']

            # Define the scatter Plot Size
            plt.figure(figsize=(10, 6))

            # Create the scatter plot with the GBIF (high number of points)
            sns.scatterplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Taxon',
                            style='Taxon', palette='deep')
            '''
            # # Add Jitter to increase visibility  - Not Helping with Visibility - Turning Off
            # sns.stripplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Taxon', palette='deep',
            #               jitter=True, alpha=0.5, native_scale=True)
            '''
            #######################
            # Overlay the PCM Plots
            #######################

            # #Define the Style Mappings for the PCM Overlay
            size_mapping = {'PCM': 50}
            color_mapping = {'PCM': '#000000'}

            # Create the scatter plot PCM only plots
            scatterPlot = sns.scatterplot(data=onlyPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source', size='Source',
                            sizes=size_mapping, palette=color_mapping)


            #For Legend get the list of unique GBIF Taxon
            new_labels = notPCMDF['Taxon'].unique().tolist()

            #Add the PCM Plots label
            new_labels.append('PCM Plots')

            #Pass the new Labels/handles to the Legend
            handles, labels = scatterPlot.get_legend_handles_labels()
            scatterPlot.legend(handles=handles, labels=new_labels)

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
            # Get max value in not PCMDF Dataframe, should get the hightest value in the graph in nearly all cases
            columns_to_include = [deficitFieldsHist, deficitFieldsHist, aetFieldsFut, aetFieldsHist]
            max_val = notPCMDF[columns_to_include].max().max()
            plt.plot([max_val, 0], [0, max_val], linestyle='--', color='black')

            plt.xlabel('Avg. Total Annual Deficit (mm)')
            plt.ylabel('Avg. Total Annual AET (mm)')

            titleLU = f'{vegNameLU} - PCM change from {timePeriodHist} to {timePeriodFut}'
            plt.title(titleLU)

            # Name for output graph
            outPDF = f'{vegNameLU}_PCMVector_{timePeriodHist}_{timePeriodFut}_GBIF_Historic.pdf'

            #OutFolder
            outFolder = 'VectorPCM_GBIFHistoric_wTaxon'

            # Full Path
            outPath = f'{outDir}\\{outFolder}\\{outPDF}'

            # Delete File IF Exists
            if os.path.exists(outPath):
                os.remove(outPath)
            # Make points file if needed
            if os.path.exists(f'{outDir}\\{outFolder}'):
                pass
            else:
                os.makedirs(f'{outDir}\\{outFolder}')

            # Export Plot
            plt.savefig(outPath, format='pdf')

            # Close Plot
            plt.close()

            messageTime = timeFun()
            scriptMsg = f'Successfully created Vector graph - {vegTypeLU} for PCM, points GBIF Historic with Taxonomy - see - {outPath} - {messageTime}'
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        return 'success function'

    except:
        print(f'Failed - vectorPCMPointsGBIFHistwTaxon')
        exit()

def vectorPCMPtsGBIFHistPerc(pointsDF, vegTypesDF, temporalDF, outDir, percentiles, percentile_colors):

    """
    Creates AET/Deficit scatter plots Vector Graphs (change from Historic to Current) by vegetation type for PCM
    plots and graphs points for GBIF historic data, with accompanying Kernel Density Estiamte (KDE)
    Function Percentile Values for the GBIF historic data.

    The KDE is the smoothed estimate of the data distribution of the points in data space
    (ie. AET and Deficit). A contour at a higher percentile level encloses a smaller area with a higher density of
    points. This is because it represents a higher threshold of density.

    :param pointsDF: points dataframe to be processed
    :param vegTypesDF: Dataframe define the Veg Types to be iterated through, this is the out loop
    :param temporalDF: Dataframe defining the Temporal Periods and associated AET and Deficit Fields.  This
    is the inner loop of the nest loop.
    :param outDir: Output directory
    :param percentiles: Kernel Density Estimate Percentile
    :param percentile_colors: Colors per Percentile.


    :return: PDFs file with scatter plot of GBIF Historic, PCM plots Historic to Mid Century, and KDE Percentiles
    for the GBIF historical AET/Deficit data.
    """
    try:

        # Prior to graphing remove records without data this will be -3.4028235E+38, all data less than 0 was set to 0
        # the 'extractAETDeficit routine
        noZeroDF = pointsDF[pointsDF['AET_Historic'] >= 0]

        # Iterate through the VegTypes
        for index, vegRow in vegTypesDF.iterrows():
            vegTypeLU = vegRow.get("VegType")
            vegNameLU = vegRow.get("VegName")

            # Define Fields for Vector Analysis from the Temporal Dataframe (should only be two record 1-Historic,
            # 2-Future

            # Get First row from temporal dataframe, will be the Historic fields
            seriesHist = temporalDF.iloc[0]
            timePeriodHist = seriesHist.get("TemporalFields")
            aetFieldsHist = seriesHist.get("AETFields")
            deficitFieldsHist = seriesHist.get("DeficitFields")

            # Get Second row from temporal dataframe, will be the Future fields
            seriesFut = temporalDF.iloc[1]
            timePeriodFut = seriesFut.get("TemporalFields")
            aetFieldsFut = seriesFut.get("AETFields")
            deficitFieldsFut = seriesFut.get("DeficitFields")

            # Subset to only the vegetation of Interest
            noZeroVegTypeDF = noZeroDF[noZeroDF['VegType'] == vegTypeLU]

            # Subset to all other than 'PCM' records
            notPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] != 'PCM']

            # Subset only PCM records
            onlyPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] == 'PCM']

            # Define the scatter Plot Size
            plt.figure(figsize=(10, 6))

            # Create the scatter plot with the GBIF (high number of points)
            sns.scatterplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Taxon',
                            style='Taxon', palette='deep')

            #######################
            # Overlay the PCM Plots
            #######################

            # #Define the Style Mappings for the PCM Overlay
            size_mapping = {'PCM': 50}
            color_mapping = {'PCM': '#000000'}

            # Create the scatter plot PCM only plots
            scatterPlot = sns.scatterplot(data=onlyPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source',
                                          size='Source',
                                          sizes=size_mapping, palette=color_mapping)

            #For Legend get the list of unique GBIF Taxon
            new_labels = notPCMDF['Taxon'].unique().tolist()

            # Add the PCM Plots label
            new_labels.append('PCM Plots')

            # Pass the new Labels/handles to the Legend
            handles, labels = scatterPlot.get_legend_handles_labels()
            #scatterPlot.legend(handles=handles, labels=new_labels)

            # Draw vectors GBIF - iterate through the dataframe sequentially
            for i in range(len(onlyPCMDF)):
                plt.plot(
                    [onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[deficitFieldsFut].values[i]],
                    [onlyPCMDF[aetFieldsHist].values[i], onlyPCMDF[aetFieldsFut].values[i]],
                    color='#000000',
                    lw=1
                )

                #Add arrow
                plt.annotate(
                    '',
                    xy=(onlyPCMDF[deficitFieldsFut].values[i], onlyPCMDF[aetFieldsFut].values[i]),
                    xytext=(onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[aetFieldsHist].values[i]),
                    arrowprops=dict(arrowstyle="->", color='#000000', lw=1)
                )

            # Add 1:1 dashed line
            # Get max value in not PCMDF Dataframe, should get the hightest value in the graph in nearly all cases
            columns_to_include = [deficitFieldsHist, deficitFieldsHist, aetFieldsFut, aetFieldsHist]
            max_val = notPCMDF[columns_to_include].max().max()
            plt.plot([max_val, 0], [0, max_val], linestyle='--', color='black')

            ######################
            # Calculate Percentile Contours
            ######################

            # Calculate the point density
            xy = np.vstack([notPCMDF[deficitFieldsHist], notPCMDF[aetFieldsHist]])
            kde = gaussian_kde(xy)

            # Define the grid over which to evaluate the KDE
            y_grid = np.linspace(notPCMDF[aetFieldsHist].min(), notPCMDF[aetFieldsHist].max(), 100)
            x_grid = np.linspace(notPCMDF[deficitFieldsHist].min(), notPCMDF[deficitFieldsHist].max(), 100)
            X, Y = np.meshgrid(x_grid, y_grid)
            positions = np.vstack([X.ravel(), Y.ravel()])
            Z = kde(positions).reshape(X.shape)

            #Create contour lines for specific percentiles with different colors
            for percentile, color in percentile_colors.items():
                level = np.percentile(Z.ravel(), percentile)
                contour = plt.contour(X, Y, Z, levels=[level], colors=[color], linestyles='dashed', linewidths=3,
                                      label=f'{percentile}th Percentile')
                handles.append(
                    Line2D([0], [0], color=color, linestyle='dashed', linewidth=3, label=f'{percentile}th Percentile'))

            new_labels += [f'{percentile}th KDE Percentile' for percentile in percentiles]

            #Add the Legend
            scatterPlot.legend(handles=handles, labels=new_labels)

            plt.xlabel('Avg. Total Annual Deficit (mm)')
            plt.ylabel('Avg. Total Annual AET (mm)')

            titleLU = f'{vegNameLU} - PCM change from {timePeriodHist} to {timePeriodFut}'
            plt.title(titleLU)

            # Name for output graph
            outPDF = f'{vegNameLU}_PCMVector_{timePeriodHist}_{timePeriodFut}_GBIF_wPercentile.pdf'

            # OutFolder
            outFolder = 'VectorPCM_GBIFHistoric_wPercentile'

            # Full Path
            outPath = f'{outDir}\\{outFolder}\\{outPDF}'

            # Delete File IF Exists
            if os.path.exists(outPath):
                os.remove(outPath)
            # Make points file if needed
            if os.path.exists(f'{outDir}\\{outFolder}'):
                pass
            else:
                os.makedirs(f'{outDir}\\{outFolder}')

            #Add Grid
            plt.grid(True)

            # Export Plot
            plt.savefig(outPath, format='pdf')

            # Close Plot
            plt.close()

            messageTime = timeFun()
            scriptMsg = f'Successfully created Graph - {vegTypeLU} for PCM, points GBIF Historic w Percentile - see - {outPath} - {messageTime}'
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        return 'success function'

    except:
        print(f'Failed - vectorPCMPtsGBIFHistPerc')
        traceback.print_exc(file=sys.stdout)
        exit()


def vectorPCMPointsGBIFHistwTaxonWWHD(pointsDF, vegTypesDF, temporalDF, figSize, outDir):

    """
    Creates AET/Deficit scatter plots Vector Graphs (change from Historic to Current) by vegetation type for PCM
    plots and graphs points for GBIF historic data in a 2 x 2 figure.  Graph symbology includes GBIF Taxon (i.e. Taxon by Veg Type).
    Vectors include Ensemble Mean, Warm Wet and Hot Dry, and the lower right is a map of the points in the scatterplots.

    2x2 scatter plot figure Ensembe, Warm Wet, Hot Dry, Spatial Points

    :param pointsDF: points dataframe to be processed
    :param vegTypesDF: Dataframe define the Veg Types to be iterated through, this is the out loop
    :param temporalDF: Dataframe defining the Temporal Periods and associated AET and Deficit Fields.  This
    is the inner loop of the nest loop.
    :param figSize: Define the size in inches of the created figure
    :param outDir: Output directory

    :return: PDFs file with Vector plots AET/Deficit per Veg Type (i.e. community) for PCM plots with Historical
     GBIF points being graphed. Exported to the 'VectorPCM_GBIFHistoricPts' directory.
    """
    try:

        # Prior to graphing remove records without data this will be -3.4028235E+38, all data less than 0 was set to 0
        # the 'extractAETDeficit routine
        noZeroDF = pointsDF[pointsDF['AET_Historic'] >= 0]

        # Iterate through the VegTypes
        for index, vegRow in vegTypesDF.iterrows():
            vegTypeLU = vegRow.get("VegType")
            vegNameLU = vegRow.get("VegName")

            # Define Fields for Vector Analysis from the Temporal Dataframe (should only be two record 1-Historic,
            # 2-Future
            # Get First row from temporal dataframe, will be the Historic fields
            seriesHist = temporalDF.iloc[0]
            timePeriodHist = seriesHist.get("TemporalFields")
            aetFieldsHist = seriesHist.get("AETFields")
            deficitFieldsHist = seriesHist.get("DeficitFields")

            # Get Second row from temporal dataframe, will be the Ensemble Fields
            seriesFut_Ens = temporalDF.iloc[1]
            timePeriodFut_Ens = seriesFut_Ens.get("TemporalFields")
            aetFieldsFut_Ens = seriesFut_Ens.get("AETFields")
            deficitFieldsFut_Ens = seriesFut_Ens.get("DeficitFields")

            # Get Third row from temporal dataframe, will be the Warm Wet Fields
            seriesFut_WW = temporalDF.iloc[2]
            timePeriodFut_WW = seriesFut_WW.get("TemporalFields")
            aetFieldsFut_WW = seriesFut_WW.get("AETFields")
            deficitFieldsFut_WW = seriesFut_WW.get("DeficitFields")

            # Get Fourth row from temporal dataframe, will be the Hot Dry Fields
            seriesFut_HD = temporalDF.iloc[3]
            timePeriodFut_HD = seriesFut_HD.get("TemporalFields")
            aetFieldsFut_HD = seriesFut_HD.get("AETFields")
            deficitFieldsFut_HD = seriesFut_HD.get("DeficitFields")

            # Subset to only the vegetation of Interest
            noZeroVegTypeDF = noZeroDF[noZeroDF['VegType'] == vegTypeLU]

            # Subset to all other than 'PCM' records
            notPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] != 'PCM']

            # Subset only PCM records
            onlyPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] == 'PCM']

            ###########################
            # Start Constructing Figure
            ###########################

            # Define the scatter Plot Size
            fig, axs = plt.subplots(2, 2, figsize=(int(figSize[0]), int(figSize[1])))

            #####
            # Figure 1 - Upper Left - Ensemble
            ######
            # Create the scatter plot with the GBIF (high number of points)
            sns.scatterplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Taxon',
                            style='Taxon', palette='coolwarm', ax=axs[0, 0])

            # Overlay the PCM Plots
            # Define the Style Mappings for the PCM Overlay
            size_mapping = {'PCM': 50}
            color_mapping = {'PCM': '#000000'}

            # Create the scatter plot PCM only plots
            scatterPlot = sns.scatterplot(data=onlyPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source', size='Source',
                            sizes=size_mapping, palette=color_mapping, ax=axs[0,0])

            # For Legend get the list of unique GBIF Taxon
            new_labels = notPCMDF['Taxon'].unique().tolist()

            # Add the PCM Plots label
            new_labels.append('PCM Plots')

            # Pass the new Labels/handles to the Legend
            handles, labels = scatterPlot.get_legend_handles_labels()
            scatterPlot.legend(handles=handles, labels=new_labels)

            # Draw vectors GBIF - iterate through the dataframe sequentially
            for i in range(len(onlyPCMDF)):
                # Ensemble
                axs[0, 0].plot(
                    [onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[deficitFieldsFut_Ens].values[i]],
                    [onlyPCMDF[aetFieldsHist].values[i], onlyPCMDF[aetFieldsFut_Ens].values[i]],
                    color='#000000',
                    lw=1.0
                )

                # Add arrow
                axs[0, 0].annotate(
                    '',
                    xy=(onlyPCMDF[deficitFieldsFut_Ens].values[i], onlyPCMDF[aetFieldsFut_Ens].values[i]),
                    xytext=(onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[aetFieldsHist].values[i]),
                    arrowprops=dict(arrowstyle="->", color='#000000', lw=1.0)
                )

            # Add 1:1 dashed line
            # Get max value in not PCMDF Dataframe, should get the highest value in the graph in nearly all cases
            columns_to_include = [deficitFieldsHist, aetFieldsHist, deficitFieldsFut_Ens, aetFieldsFut_Ens]
            max_val = notPCMDF[columns_to_include].max().max()
            axs[0, 0].plot([max_val, 0], [0, max_val], linestyle='--', color='black')

            # Add Title
            axs[0, 0].set_title('Ensemble 2040-2069')

            #####.
            # Figure 2- Lower Right - Warm Wet
            ######
            # Create the scatter plot with the GBIF (high number of points)
            sns.scatterplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Taxon',
                            style='Taxon', palette='coolwarm', ax=axs[1, 0])

            # Overlay the PCM Plots
            # Define the Style Mappings for the PCM Overlay
            size_mapping = {'PCM': 50}
            color_mapping = {'PCM': '#000000'}

            # Create the scatter plot PCM only plots
            scatterPlot = sns.scatterplot(data=onlyPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source',
                                          size='Source',
                                          sizes=size_mapping, palette=color_mapping, ax=axs[1, 0])

            # For Legend get the list of unique GBIF Taxon
            new_labels = notPCMDF['Taxon'].unique().tolist()

            # Add the PCM Plots label
            new_labels.append('PCM Plots')

            # Pass the new Labels/handles to the Legend
            handles, labels = scatterPlot.get_legend_handles_labels()
            scatterPlot.legend(handles=handles, labels=new_labels)

            # Draw vectors GBIF - iterate through the dataframe sequentially
            for i in range(len(onlyPCMDF)):
                # Warm Wet
                axs[1, 0].plot(
                    [onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[deficitFieldsFut_WW].values[i]],
                    [onlyPCMDF[aetFieldsHist].values[i], onlyPCMDF[aetFieldsFut_WW].values[i]],
                    color='#000000',
                    lw=1.0
                )

                # Add arrow
                axs[1, 0].annotate(
                    '',
                    xy=(onlyPCMDF[deficitFieldsFut_WW].values[i], onlyPCMDF[aetFieldsFut_WW].values[i]),
                    xytext=(onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[aetFieldsHist].values[i]),
                    arrowprops=dict(arrowstyle="->", color='#000000', lw=1.0)
                )

            # Add 1:1 dashed line
            # Get max value in not PCMDF Dataframe, should get the highest value in the graph in nearly all cases
            # using the Ensemble to be consistent acorss figures
            columns_to_include = [deficitFieldsHist, aetFieldsHist, deficitFieldsFut_Ens, aetFieldsFut_Ens]
            max_val = notPCMDF[columns_to_include].max().max()
            axs[1, 0].plot([max_val, 0], [0, max_val], linestyle='--', color='black')

            # Add Title
            axs[1, 0].set_title('Warm Wet 2040-2069')

            #####
            # Figure 3- Upper Right - Hot Dry
            ######
            # Create the scatter plot with the GBIF (high number of points)
            sns.scatterplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Taxon',
                            style='Taxon', palette='coolwarm', ax=axs[0, 1])

            # Overlay the PCM Plots
            # Define the Style Mappings for the PCM Overlay
            size_mapping = {'PCM': 50}
            color_mapping = {'PCM': '#000000'}

            # Create the scatter plot PCM only plots
            scatterPlot = sns.scatterplot(data=onlyPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source',
                                          size='Source',
                                          sizes=size_mapping, palette=color_mapping, ax=axs[0, 1])

            # For Legend get the list of unique GBIF Taxon
            new_labels = notPCMDF['Taxon'].unique().tolist()

            # Add the PCM Plots label
            new_labels.append('PCM Plots')

            # Pass the new Labels/handles to the Legend
            handles, labels = scatterPlot.get_legend_handles_labels()
            scatterPlot.legend(handles=handles, labels=new_labels)

            # Draw vectors GBIF - iterate through the dataframe sequentially
            for i in range(len(onlyPCMDF)):
                # Hot Dry
                axs[0, 1].plot(
                    [onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[deficitFieldsFut_HD].values[i]],
                    [onlyPCMDF[aetFieldsHist].values[i], onlyPCMDF[aetFieldsFut_HD].values[i]],
                    color='#000000',
                    lw=1.0
                )

                # Add arrow
                axs[0, 1].annotate(
                    '',
                    xy=(onlyPCMDF[deficitFieldsFut_HD].values[i], onlyPCMDF[aetFieldsFut_HD].values[i]),
                    xytext=(onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[aetFieldsHist].values[i]),
                    arrowprops=dict(arrowstyle="->", color='#000000', lw=1.0)
                )

            # Add 1:1 dashed line
            # Get max value in not PCMDF Dataframe, should get the highest value in the graph in nearly all cases
            # using the Ensemble to be consistent across figures
            columns_to_include = [deficitFieldsHist, aetFieldsHist, deficitFieldsFut_Ens, aetFieldsFut_Ens]
            max_val = notPCMDF[columns_to_include].max().max()
            axs[0, 1].plot([max_val, 0], [0, max_val], linestyle='--', color='black')

            # Add Title
            axs[0, 1].set_title('Hot Dry 2040-2069')

            # Set common axis labels all plots, lower left will be overwritten during map creation.
            for ax in axs.flat:
                ax.set(xlabel='Avg. Total Annual Deficit (mm)', ylabel='Avg. Total Annual AET (mm)')

            #####
            # Figure 4- Lower Right - Map of Points GBIF and PCM by Community
            ######

            # Set PCM plots to 'Taxon = PCM
            onlyPCMDF.loc[onlyPCMDF['Source'] == 'PCM', 'Taxon'] = 'PCM Plot'

            # Convert DataFrame with Veg to aa GeoDataFrame be sure the extracted points are from GCS WGS 1984
            # Only PCM GDF
            gdfPCMOnly = gpd.GeoDataFrame(
                onlyPCMDF,
                geometry=gpd.points_from_xy(onlyPCMDF.Longitude, onlyPCMDF.Latitude),
                crs="EPSG:4326")

            # Only PCM GDF
            gdfGBIFOnly = gpd.GeoDataFrame(
                notPCMDF,
                geometry=gpd.points_from_xy(notPCMDF.Longitude, notPCMDF.Latitude),
                crs="EPSG:4326")

            # Create color palette matching the scatter plot symbology
            unique_taxon = notPCMDF['Taxon'].unique()
            palette = sns.color_palette('coolwarm', len(unique_taxon))
            color_dict = dict(zip(unique_taxon, palette))

            # Add 'PCM Plots' with a specific color (e.g., black)
            color_dict['PCM Plots'] = '#000000'

            # Add a map with topographic background to subplot [1, 1]
            ax = axs[1, 1]

            # Set  longitude and latitude bounding boxes to CONUS
            axs[1, 1].set_xlim([-127, -66])  # Set longitude bounds
            axs[1, 1].set_ylim([25, 50])  # Set latitude bounds

            # # Plot GBIF Points First
            # gdfGBIFOnly.plot(ax=ax, marker='o', c='Taxon', cmap='tab10', legend=True)
            #
            # # Plot the PCM Plots Second so if on top and visible
            # gdfPCMOnly.plot(ax=ax, marker='o', color='black', markersize=100, label='PCM Plots')

            gdfGBIFOnly['color'] = gdfGBIFOnly['Taxon'].map(color_dict)  # Map 'Taxon' to color
            gdfGBIFOnly.plot(ax=ax, marker='o', color=gdfGBIFOnly['color'], markersize=10, legend=True)

            # Plot the PCM Plots Second so they are on top and visible
            gdfPCMOnly.plot(ax=ax, marker='o', color='black', markersize=25, label='PCM Plots')

            # Add a topographic background
            ctx.add_basemap(ax, crs=gdfGBIFOnly.crs.to_string(), source=ctx.providers.CartoDB.Voyager)

            # Set title and labels
            ax.set_title(f'GBIF Taxon and PCM Plots for - {vegNameLU}')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')

            # # Create a custom legend for the map matching the scatter plot symbology
            # handles = [plt.Line2D([0], [0], marker='o', color=color_dict[taxon], linestyle='None', markersize=10,
            #                       label=taxon)
            #            for taxon in unique_taxon]  # For GBIF points
            #
            # handles.append(plt.Line2D([0], [0], marker='o', color='black', linestyle='None', markersize=10,
            #                           label='PCM Plots'))  # For PCM Plots

            # Create a custom legend for the map matching the scatter plot symbology
            handles = [plt.Line2D([0], [0], marker='o', color=color_dict[taxon], linestyle='None', markersize=10,
                                  label=taxon) for taxon in unique_taxon]  # For GBIF points

            handles.append(plt.Line2D([0], [0], marker='o', color='black', linestyle='None', markersize=10,
                                      label='PCM Plots'))  # For PCM Plots

            # Add the legend to the map subplot
            ax.legend(handles=handles, title=None, loc='best')

            #########################
            # Attributes Whole Figure
            #########################

            # Add Overall Figure Title
            titleLU = f'{vegNameLU} - PCM change from {timePeriodHist} to 2040 - 2069'
            fig.suptitle(titleLU, fontsize=16)

            # Name for output graph
            outPDF = f'{vegNameLU}_PCMVector_{timePeriodHist}_2040_2069_EnsembleWWHD.pdf'

            # OutFolder
            outFolder = 'VectorPCM_GBIFHistoric_wTaxon_EnsWWHD'

            # Full Path
            outPath = f'{outDir}\\{outFolder}\\{outPDF}'

            # Delete File IF Exists
            if os.path.exists(outPath):
                os.remove(outPath)
            # Make points file if needed
            if os.path.exists(f'{outDir}\\{outFolder}'):
                pass
                pass
            else:
                os.makedirs(f'{outDir}\\{outFolder}')

            # Export Plot
            plt.savefig(outPath, format='pdf')

            # Close Plot
            plt.close()

            messageTime = timeFun()
            scriptMsg = f'Successfully created Vector graph - {vegTypeLU} for vectorPCMPointsGBIFTaxonWWHD - see - {outPath} - {messageTime}'
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        return 'success function'

    except:
        print(f'Failed - vectorPCMPointsGBIFHistwTaxon')
        exit()

def vectorPCMPointsGBIFHistwTaxonWWHDOne(pointsDF, vegTypesDF, temporalDF, figSize, outDir):

    """
    Creates AET/Deficit scatter plots Vector Graphs (change from Historic to Current) by vegetation type for PCM
    plots and graphs points for GBIF historic data in a 1x1 figure (i.e. on figure).
    Graph symbology includes GBIF Taxon (i.e. Taxon by Veg Type). Vectors include Warm Wet and Hot Dry only.

    Figure being created for the CCAM Manuscript



    :param pointsDF: points dataframe to be processed
    :param vegTypesDF: Dataframe define the Veg Types to be iterated through, this is the out loop
    :param temporalDF: Dataframe defining the Temporal Periods and associated AET and Deficit Fields.  This
    is the inner loop of the nest loop.
    :param figSize: Define the size in inches of the created figure
    :param outDir: Output directory

    :return: PDFs file with Vector plots AET/Deficit per Veg Type (i.e. community) for PCM plots with Historical
     GBIF points being graphed. Exported to the 'VectorPCM_GBIFHistoricPts' directory.
    """
    try:

        #Over riding the defined figure size in setup
        figSize = [10, 6]


        # Prior to graphing remove records without data this will be -3.4028235E+38, all data less than 0 was set to 0
        # the 'extractAETDeficit routine
        noZeroDF = pointsDF[pointsDF['AET_Historic'] >= 0]

        # Iterate through the VegTypes
        for index, vegRow in vegTypesDF.iterrows():
            vegTypeLU = vegRow.get("VegType")
            vegNameLU = vegRow.get("VegName")

            # Define Fields for Vector Analysis from the Temporal Dataframe (should only be two record 1-Historic,
            # 2-Future
            # Get First row from temporal dataframe, will be the Historic fields
            seriesHist = temporalDF.iloc[0]
            timePeriodHist = seriesHist.get("TemporalFields")
            aetFieldsHist = seriesHist.get("AETFields")
            deficitFieldsHist = seriesHist.get("DeficitFields")

            # Get Second row from temporal dataframe, will be the Ensemble Fields
            seriesFut_Ens = temporalDF.iloc[1]
            timePeriodFut_Ens = seriesFut_Ens.get("TemporalFields")
            aetFieldsFut_Ens = seriesFut_Ens.get("AETFields")
            deficitFieldsFut_Ens = seriesFut_Ens.get("DeficitFields")

            # Get Third row from temporal dataframe, will be the Warm Wet Fields
            seriesFut_WW = temporalDF.iloc[2]
            timePeriodFut_WW = seriesFut_WW.get("TemporalFields")
            aetFieldsFut_WW = seriesFut_WW.get("AETFields")
            deficitFieldsFut_WW = seriesFut_WW.get("DeficitFields")

            # Get Fourth row from temporal dataframe, will be the Hot Dry Fields
            seriesFut_HD = temporalDF.iloc[3]
            timePeriodFut_HD = seriesFut_HD.get("TemporalFields")
            aetFieldsFut_HD = seriesFut_HD.get("AETFields")
            deficitFieldsFut_HD = seriesFut_HD.get("DeficitFields")

            # Subset to only the vegetation of Interest
            noZeroVegTypeDF = noZeroDF[noZeroDF['VegType'] == vegTypeLU]

            # Subset to all other than 'PCM' records
            notPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] != 'PCM']

            # Subset only PCM records
            onlyPCMDF = noZeroVegTypeDF[noZeroVegTypeDF['Source'] == 'PCM']

            ###########################
            # Start Constructing Figure
            ###########################

            # Define the scatter Plot Size
            fig, axs = plt.subplots(1, 1, figsize=(int(figSize[0]), int(figSize[1])))

            #####
            # Figure 1 - Upper Left - Ensemble
            ######
            # Create the scatter plot with the GBIF (high number of points)
            sns.scatterplot(data=notPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Taxon',
                            style='Taxon', palette='coolwarm')

            # Overlay the PCM Plots
            # Define the Style Mappings for the PCM Overlay
            size_mapping = {'PCM': 50}
            color_mapping = {'PCM': '#000000'}

            # Create the scatter plot PCM only plots
            scatterPlot = sns.scatterplot(data=onlyPCMDF, x=deficitFieldsHist, y=aetFieldsHist, hue='Source', size='Source',
                            sizes=size_mapping, palette=color_mapping)

            # For Legend get the list of unique GBIF Taxon
            new_labels = notPCMDF['Taxon'].unique().tolist()

            # Add the PCM Plots label
            new_labels.append('Plots')

            ################
            # Legend Section
            ################
            # Pass the new Labels/handles to the Legend
            handles, labels = scatterPlot.get_legend_handles_labels()
            #scatterPlot.legend(handles=handles, labels=new_labels)

            # Existing legend handles and labels
            handles, labels = scatterPlot.get_legend_handles_labels()

            # Replace the PCM with the 'Monitoring Plots'
            labels = ['Monitoring Plots' if label == 'PCM' else label for label in labels]

            # Create custom line handles for vectors
            ww_handle = mlines.Line2D([], [], color='#0000ff', lw=1.5, label='Warm Wet Change', linestyle='-',
                                      marker=r'$\rightarrow$', markersize=8, markerfacecolor='#0000ff')
            hd_handle = mlines.Line2D([], [], color='#ff0000', lw=1.5, label='Hot Dry Change', linestyle='-',
                                      marker=r'$\rightarrow$', markersize=8, markerfacecolor='#ff0000')

            # Add to handles
            handles.append(ww_handle)
            handles.append(hd_handle)
            labels.append('Warm Wet Change')
            labels.append('Hot Dry Change')

            # Re-draw the legend
            scatterPlot.legend(handles=handles, labels=labels, loc='best', fontsize='small')

            # Draw vectors GBIF Hot Dry first will have great length easier to see then Warm Wet
            for i in range(len(onlyPCMDF)):

                axs.plot(
                    [onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[deficitFieldsFut_HD].values[i]],
                    [onlyPCMDF[aetFieldsHist].values[i], onlyPCMDF[aetFieldsFut_HD].values[i]],
                    color='#ff0000',
                    lw=1.0
                )

                # Add arrow
                axs.annotate(
                    '',
                    xy=(onlyPCMDF[deficitFieldsFut_HD].values[i], onlyPCMDF[aetFieldsFut_HD].values[i]),
                    xytext=(onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[aetFieldsHist].values[i]),
                    arrowprops=dict(arrowstyle="->", color='#ff0000', lw=1.0)
                )

            # Draw vectors GBIF Warm Wet
            for i in range(len(onlyPCMDF)):
                # Ensemble
                axs.plot(
                    [onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[deficitFieldsFut_WW].values[i]],
                    [onlyPCMDF[aetFieldsHist].values[i], onlyPCMDF[aetFieldsFut_WW].values[i]],
                    color='#0000ff',
                    lw=1.0
                )

                # Add arrow
                axs.annotate(
                    '',
                    xy=(onlyPCMDF[deficitFieldsFut_WW].values[i], onlyPCMDF[aetFieldsFut_WW].values[i]),
                    xytext=(onlyPCMDF[deficitFieldsHist].values[i], onlyPCMDF[aetFieldsHist].values[i]),
                    arrowprops=dict(arrowstyle="->", color='#0000ff', lw=1.0)
                )

            # Add 1:1 dashed line
            # Get max value in not PCMDF Dataframe, should get the highest value in the graph in nearly all cases
            columns_to_include = [deficitFieldsHist, aetFieldsHist, deficitFieldsFut_Ens, aetFieldsFut_Ens]
            max_val = notPCMDF[columns_to_include].max().max()
            axs.plot([max_val, 0], [0, max_val], linestyle='--', color='black')

            # Set common axis labels all plots, lower left will be overwritten during map creation.
            for ax in np.atleast_1d(axs).flat:
                ax.set(xlabel='Avg. Total Annual Deficit (mm)', ylabel='Avg. Total Annual AET (mm)')



            #########################
            # Attributes Whole Figure
            #########################

            # Add Overall Figure Title
            titleLU = f'{vegNameLU} change from {timePeriodHist} to 2040-2069'
            fig.suptitle(titleLU, fontsize=16, y=0.93)

            # Name for output graph
            outPDF = f'{vegNameLU}_PCMVector_{timePeriodHist}_2040_2069_WWHD.pdf'

            # OutFolder
            outFolder = 'VectorPCM_GBIFHistoric_wTaxon_EnsWWHDOne'

            # Full Path
            outPath = f'{outDir}\\{outFolder}\\{outPDF}'

            # Delete File IF Exists
            if os.path.exists(outPath):
                os.remove(outPath)
            # Make points file if needed
            if os.path.exists(f'{outDir}\\{outFolder}'):
                pass
                pass
            else:
                os.makedirs(f'{outDir}\\{outFolder}')

            # Export Plot
            plt.savefig(outPath, format='pdf')

            # Close Plot
            plt.close()

            messageTime = timeFun()
            scriptMsg = f'Successfully created Vector graph - {vegTypeLU} for vectorPCMPointsGBIFTaxonWWHDOne - see - {outPath} - {messageTime}'
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        return 'success function'

    except:
        print(f'Failed - vectorPCMPointsGBIFHistwTaxon')
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