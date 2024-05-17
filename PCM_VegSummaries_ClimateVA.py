"""
PCM_VegSummaries_ClimateVA.py
Script performs summary of the NAWMA Vegeetation Plot of the Plant Communities Monitoring Protocol.
Summary is to be used for identifying highest cover taxon by PCM Community Type to be used in as
part of the SFAN PCM Climate Change Vulnerability Assessment.

Output tables:
NAWMACoverEventALL - All Taxon Average Cover By Event/Plot Scale
NAWMACoverEventTopTwoTop - Two Taxon Highest Average Cover By Event/Plot Scale
NAWMACoverMonCycleAll - All Taxon Average Cover By Community Monitoring Cycle Scale
NAWMACoverMonCycleTopTwo - Top Two Taxon Highest Average Cover By Community Monitoring Cycle Scale
NAWMACoverCommunity - All Taxon Average Cover By Community across all monitoring cycles
NAWMACoverCommunityTopTwo - Top Two Taxon Highest Average Cover By Community across all monitoring cycles

Python Environment: PCM_VegClimateVA - Python 3.11

Date Developed - May 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""

# Import Libraries
import pandas as pd
import pyodbc
import sys
import os
import session_info
import traceback
from datetime import datetime

# PCM Database
inDB = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\PlantCommunities\Data\Database\SFAN_PlantCommunities_BE_20240306.accdb'

# List of Taxon/Species to be removed from analysis
taxonRemoveList = ['Litter', 'Bare Ground', 'Lichen']

# Output Name, OutDir, and Workspace
outName = 'PCM_NAWMA_Vegetation_ClimateVA'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\PCM\NAWMACover'  # Directory Output Location
workspace = f'{outDir}\\workspace'  # Workspace Output Directory
dateNow = datetime.now().strftime('%Y%m%d')
logFileName = f'{workspace}\\{outName}_{dateNow}.LogFile.txt'  # Name of the .txt script logfile which is saved in the workspace directory

def main():
    try:
        session_info.show()
        # Set option in pandas to not allow chaining (views) of dataframes, instead force copy to be performed.
        pd.options.mode.copy_on_write = True
        ################
        # IMPORT Datasets
        ################

        inQuery = "Select * FROM tblNAWMADataset"
        outFun = connect_to_AcessDB(inQuery, inDB)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - NAWMADataset" + messageTime + " - Failed - Exiting Script")
            exit()

        messageTime = timeFun()
        print(f'Success: connect_to_AcessDB - NAWMADataset {messageTime}')
        outDfNAWMA = outFun[1]

        # Import the Events Dataset
        inQuery = "Select * FROM tblEventsDataset"
        outFun = connect_to_AcessDB(inQuery, inDB)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - EventsDataset" + messageTime + " - Failed - Exiting Script")
            exit()

        messageTime = timeFun()
        print(f'Success: connect_to_AcessDB - EventDataset {messageTime}')
        # Events dataset dataframe
        DfEvents = outFun[1]

        ####################
        # Calculate NAWMA Site Percentage Cover By Species By Location Event - Pulling from tblNAWMADataset
        ####################

        outFun = NAWMA_CoverByEvent(outDfNAWMA, taxonRemoveList)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print(
                "WARNING - Function connect_to_AcessDB - nawma_CoverByEvent" + messageTime + " - Failed - Exiting Script")
            exit()
        DFWithAverageCoverByEvent = outFun[1]

        # Join Event to Event Scale Summary
        # Define Field(s) to do join on
        joinFields = ['EventID']
        # Retain only the fields of interest
        fieldsToRetain = ['UnitCode', 'EventID', 'StartDate', 'LocationID', 'LocName', 'Latitude', 'Longitude',
                          'VegCode', 'VegDescription', 'Species', 'TotalCover', 'PlotCount', 'AverageCover']

        outFun = joinWEventDataset(DFWithAverageCoverByEvent, DfEvents, joinFields, fieldsToRetain)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function joinWEventDataset - Event Scale " + messageTime + " - Failed - Exiting Script")
            exit()

        DFCoverEvent = outFun[1]
        outDFList = []
        outDFList.append(DFCoverEvent)
        outDFListName = []
        outDFListName.append('NAWMACoverEventALL')

        scriptMsg = 'Successfully Completed Event Scale NAWMA Cover Summaries ' + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        ########################################
        # NAWMA_CoverByMonCycle
        ########################################
        outFun = NAWMA_HighestCoverByEvent(DFCoverEvent)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function NAWMA_HighestCoverByEvent - " + messageTime + " - Failed - Exiting Script")
            exit()

        DFCoverEventTopTwo = outFun[1]  # Export to  excel

        outDFList.append(DFCoverEventTopTwo)
        outDFListName.append('NAWMACoverEventTopTwo')

        # Derive the Top Highest Cover Taxonomy By Community Monitoring Cycle

        # Calculate NAWMA Community Monitoring Cycle Average Percent Cover (i.e. Sum of All Taxon Percent Cover divided
        # by the number of plots in the monitoring cycle).
        outFun = NAWMA_CoverByMonCycle(outDfNAWMA, taxonRemoveList, DfEvents)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print(
                "WARNING -  - NAWMA_CoverByMonCycle" + messageTime + " - Failed - Exiting Script")
            exit()
        DFCoverMonCycle = outFun[1]
        # Export all Cover by Monitoring Cycle
        outDFList.append(DFCoverMonCycle)
        outDFListName.append('NAWMACoverMonCycleAll')

        # Derive the Top Two Cover by Monitoring Cycle
        outFun = NAWMA_HighestCoverByMonCycle(DFCoverMonCycle)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print(
                "WARNING - Function NAWMA_HighestCoverByMonCycle - " + messageTime + " - Failed - Exiting Script")
            exit()
        # Out Top Two Aveage Taxony Cover By Community Monitoring Cycle
        DFCoverMonCycleTopTwo = outFun[1]  # Export to  excel
        outDFList.append(DFCoverMonCycleTopTwo)
        outDFListName.append('NAWMACoverMonCycleTopTwo')

        scriptMsg = 'Successfully Completed Monitoring Cycle NAWMA Cover Summaries ' + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        ##########################################################
        # Derive the Top Two Highest Average Cover Taxonomy By Community across all Monitoring Cycles
        ##########################################################

        # Calculate NAWMA Community Average Percent Cover (i.e. Sum of All Taxon Percent Cover divided
        # by the number of plots across all monitoring cycles).
        outFun = NAWMA_CoverByCommunity(outDfNAWMA, taxonRemoveList, DfEvents)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print(
                "WARNING -  - NAWMA_CoverByMonCycle" + messageTime + " - Failed - Exiting Script")
            exit()
        DFCoverCommunity = outFun[1]
        # Export all Cover by Monitoring Cycle
        outDFList.append(DFCoverCommunity)
        outDFListName.append('NAWMACoverCommunity')

        # Derive the Top Two Cover by Monitoring Cycle
        outFun = NAWMA_HighestCoverByCommunity(DFCoverCommunity)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function NAWMA_HighestCoverByCommunity - " + messageTime + " - Failed - Exiting Script")
            exit()

        # Out Top Two Average Taxonomy Cover By Community
        DFCoverCommunityTopTwo = outFun[1]  # Export to  excel
        outDFList.append(DFCoverCommunityTopTwo)
        outDFListName.append('NAWMACoverCommunityTopTwo')

        scriptMsg = 'Successfully Completed Community NAWMA Cover Summaries ' + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")


        ##############################
        #Export the output Dataframes
        #############################
        #Define Path to Output Summary Table
        outExcel = f'{outDir}\\{outName}_{dateNow}.xlsx'

        if os.path.exists(outExcel):
            os.remove(outExcel)

        #Open the Pandas excel writer
        excel_writer = pd.ExcelWriter(outExcel, engine='xlsxwriter')

        # Write each DataFrame to a separate worksheet
        for i, df in enumerate(outDFList):
            sheetName = outDFListName[i]
            df.to_excel(excel_writer, sheet_name=sheetName, index=False)

        # Save the Excel file
        excel_writer.close()

        scriptMsg = 'Successfully Completed Community Scale NAWMA Cover Summaries - see output: ' + outExcel + ' - '+ messageTime

        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()


    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - PCM_VegSummaries_ClimateVA.py - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()
        traceback.print_exc(file=sys.stdout)


    finally:
        exit()

def NAWMA_HighestCoverByEvent(inDF):
    """
    Extracts the Highest Cover Taxon by Event, Community Monitoring Cycle Year, and Community Across all years.

    :param inDF: dataframe with the Event Scale Average Percent Cover and event metadata (e.g. Community type, etc.)

    :return: outSummaryDF: Data Frame with the Summary output at the Event Scale
    """

    try:
        ############################################
        # Extract the Event Scale Top Two Highest Cover Taxon
        ############################################
        # Create copy of the dataframe to be used in the Function
        inDF = inDF.copy()
        # Define the Index Field in the 'inDF' to Composite 'EventID', 'Species'
        inDF['CompositeKey'] = inDF['EventID'].astype(str) + inDF['Species']

        # Set Index to the Composite Key Field
        inDF.set_index('CompositeKey', inplace=True)

        # List of Fields to Use in the Group By
        groupList = ['EventID']
        # Get the Top Two Cover Records By Event, retaining the index value
        eventTopTwoGBDF = inDF.groupby(groupList, group_keys=False).apply(lambda x: x.nlargest(2, 'AverageCover'))

        # Push the Index Values Back to fields
        eventTopTwoGBDF.reset_index(inplace=True)
        inDF.reset_index(inplace=True)

        # Join on the 'CompositeKeyEventsSpecies' field
        outSummaryDF = pd.merge(inDF, eventTopTwoGBDF[['CompositeKey']],
                                on='CompositeKey', how='inner')

        # Drop the Composite Key Field
        outSummaryDF.drop(columns=['CompositeKey'], inplace=True)

        del eventTopTwoGBDF
        del inDF

        print(f'Successfully Processed - NAWMA_HighestCoverByEvent')
        return 'success function', outSummaryDF


    except:
        print(f'Failed - NAWMA_HighestCoverByEvent')
        exit()


def NAWMA_HighestCoverByMonCycle(inDF):
    """
    Extracts the Highest Cover Taxon by Community Monitoring Cycle Year.

    :param inDF: dataframe with the Community Monitoring Cycle Percent Cover
     and event metadata (e.g. Community type, etc.)

    :return: outSummaryDF: Data Frame with the Summary output By Community Monitoring Cycle
    """

    try:
        ############################################
        # Extract the Event Scale Top Two Highest Cover Taxon
        ############################################
        # Create copy of the dataframe to be used in the Function
        inDF = inDF.copy()

        # Define the Index Field in the 'inDF' to Composite 'EventID', 'Species'
        inDF['CompositeKey'] = inDF['VegCode'] + inDF['Year'].astype(str)

        # Set Index to the Composite Key Field
        inDF.set_index('CompositeKey', inplace=True)

        # List of Fields to Use in the Group By
        groupList = ['VegCode', 'Year']
        # Get the Top Two Cover Records By Event, retaining the index value
        outSummaryDF = inDF.groupby(groupList, group_keys=False).apply(
            lambda x: x.nlargest(2, 'MonitoringCycleAverageCover'))

        # Push the Index Values Back to fields
        outSummaryDF.reset_index(inplace=True)

        # Drop the Composite Key Field
        outSummaryDF.drop(columns=['CompositeKey'], inplace=True)

        del inDF

        print(f'Successfully Processed - NAWMA_HighestCoverByMonCycle')

        return 'success function', outSummaryDF

    except:
        print(f'Failed - NAWMA_HighestCoverByMonCycle')
        exit()

def NAWMA_HighestCoverByCommunity(inDF):
    """
    Extracts the Highest Cover Taxon by Community across all years.

    :param inDF: dataframe with the Community Average Percent Cover
     and event metadata (e.g. Community type, etc.)

    :return: outSummaryDF: Data Frame with the Summary output By Community
    """

    try:
        ############################################
        # Extract the Event Scale Top Two Highest Cover Taxon
        ############################################
        # Create copy of the dataframe to be used in the Function
        inDF = inDF.copy()

        # Set Index to the Composite Key Field
        inDF.set_index('VegCode', inplace=True)

        # List of Fields to Use in the Group By
        groupList = ['VegCode']

        # Get the Top Two Cover Records By Event, retaining the index value
        outSummaryDF = inDF.groupby(groupList, group_keys=False).apply(
            lambda x: x.nlargest(2, 'CommunityAverageCover'))

        # Push the Index Values Back to fields
        outSummaryDF.reset_index(inplace=True)

        del inDF

        print(f'Successfully Processed - NAWMA_HighestCoverByCommunity')

        return 'success function', outSummaryDF

    except:
        print(f'Failed - NAWMA_HighestCoverByCommunity')
        exit()


def joinWEventDataset(dfToJoin, dfEvents, joinFields, fieldsToRetain):
    """
    Join dataframes based on the defiend 'joinFields' list.  Subset to the fields defined in 'fieldToRetain'
    Calculate a Year field as well

    :param dfToJoin: dataframe to be joined with the event dataframe
    :param dfEvents: dataframe event data
    :param joinFields: List with the Field(s) on which the join/merge will be performed
    :param fieldToRetain: List the Fields(s) to be retained in the joined/merged output

    :return: outSummaryEvent: Dataframe with the Summary Data Frame merged with the Event Dataframe
    """

    try:
        # Join Event Dataset with AvergeCoverBy Event Dataset and Find Top Two Cover Taxon per plot type
        outSummaryEvent = pd.merge(dfToJoin, dfEvents, on=joinFields)

        # Subset to the desired fields
        outSummaryEvent = outSummaryEvent.loc[:, fieldsToRetain]

        # Calculate a Year Field
        outSummaryEvent.insert(2, 'Year', outSummaryEvent['StartDate'].dt.year)

        return 'success function', outSummaryEvent

    except:
        print(f'Failed - joinWEventDataset')
        exit()


def NAWMA_CoverByEvent(inDF, taxonRemoveList):
    """
    Create the NAWMA Site Percentage Cover By Species By Location/Event Summary - Pulling from tblNAWMADataset.
    Recreating the qrpt_NAWMA_SpeciesPercentCover summary in the PCM Front End but summary at the Event Scale rather
    then plot scale.

    :param inDF: dataframe with the tblNAWMADataset dataset files
    :param taxonRemoveList: List with the taxon in field 'Species' to be removed from analysis

    :return: nawmaDFEventSpeciesCoverPlotsByEvent: Dataframe with the Event Scale Total Cover, Plot Count and
    Average Cover fields output
    """
    try:

        # Remove NAWMA Plots - only retaining A, B, C subplots which have 50 hits per
        nawmaDFSetup = inDF[inDF['TransectID'] != 'NAWMA']

        # Removed Non-Herbaceous Taxon
        mask = nawmaDFSetup['Species'].isin(taxonRemoveList)

        # apply the Mask, removing the Non-Herbaceous records
        nawmaDFSetup = nawmaDFSetup[~mask]

        # Calculate the Percent Cover as [HitsInQuadrant] * 2 (50 sample points in the NAWMA subplots.
        nawmaDFSetup['PercentCover'] = nawmaDFSetup['HitsInQuadrat'] * 2

        # Get Number of Plots (i.e. A, B, C) by event, the norm will be three
        nawmaPlotsByEvent = nawmaDFSetup.groupby('EventID')['TransectID'].nunique().reset_index(name='PlotCount')

        # Sum the Total Cover by Event Species
        nawmaDFEventSpeciesCover = nawmaDFSetup.groupby(['EventID', 'Species'])['PercentCover'].sum().reset_index(
            name='TotalCover')

        # Join the PlotsByEvent and EventSpeciesCover Dataframes then Calculate the Species Event Percent Cover
        nawmaDFEventSpeciesCoverPlotsByEvent = pd.merge(nawmaDFEventSpeciesCover, nawmaPlotsByEvent, on='EventID')

        # Calculate the Nawma (Plots A, B, C) average cover by Event
        nawmaDFEventSpeciesCoverPlotsByEvent['AverageCover'] = nawmaDFEventSpeciesCoverPlotsByEvent['TotalCover'] / \
                                                               nawmaDFEventSpeciesCoverPlotsByEvent['PlotCount']
        del nawmaDFSetup
        del nawmaDFEventSpeciesCover
        del nawmaPlotsByEvent

        return 'success function', nawmaDFEventSpeciesCoverPlotsByEvent

    except:
        print(f'Failed - NAWMA_CoverByEvent')
        exit()


def NAWMA_CoverByMonCycle(inDF, taxonRemoveList, DfEvents):
    """
    Create the NAWMA Average Cover by Community Monitoring Cycle - Pulling from tblNAWMADataset.

    :param inDF: dataframe with the tblNAWMADataset dataset file
    :param taxonRemoveList: List with the taxon in field 'Species' to be removed from analysis
    :param DfEvents: Events Dataframe will be used to join event metadata to outputs

    :return: nawmaMonCycleTCwPC: Dataframe with the Community Monitoring Cycle Total Cover, Number of Plots and
    Average Cover fields output
    """
    try:
        # Remove NAWMA Plots - only retaining A, B, C subplots which have 50 hits per
        nawmaDFSetup = inDF[inDF['TransectID'] != 'NAWMA']

        # Removed Non-Herbaceous Taxon
        mask = nawmaDFSetup['Species'].isin(taxonRemoveList)

        # apply the Mask, removing the Non-Herbaceous records
        nawmaDFSetup = nawmaDFSetup[~mask]

        # Calculate the Percent Cover as [HitsInQuadrant] * 2 (50 sample points in the NAWMA subplots.
        nawmaDFSetup['PercentCover'] = nawmaDFSetup['HitsInQuadrat'] * 2

        # Join to Event Dataset to get Year and Community
        # Define Field(s) to do join on
        joinFields = ['EventID']
        # Retain only the fields of interest
        fieldsToRetain = ['UnitCode', 'EventID', 'StartDate', 'LocationID', 'LocName', 'Latitude', 'Longitude',
                          'TransectID', 'VegCode', 'VegDescription', 'Species', 'HitsInQuadrat', 'PercentCover']

        # Perform the Merge of nawma
        DFNAWMAwPCwEvent = pd.merge(nawmaDFSetup, DfEvents[['EventID', 'StartDate', 'VegCode', 'VegDescription']],
                                    on=joinFields)

        # Subset to the desired fields
        DFNAWMAwPCwEvent = DFNAWMAwPCwEvent.loc[:, fieldsToRetain]

        # Calculate a Year Field
        DFNAWMAwPCwEvent.insert(2, 'Year', DFNAWMAwPCwEvent['StartDate'].dt.year)

        # Get Number of Plots By Event  (i.e. A, B, C) by event, the norm will be three
        nawmaPlotCountByEvent = DFNAWMAwPCwEvent.groupby(['EventID', 'VegCode', 'Year'])[
            'TransectID'].nunique().reset_index(name='EventPlotCount')

        # Get Number of Plot by Community Monitoring Cycle
        nawmaPlotCountComMonCycle = nawmaPlotCountByEvent.groupby(['VegCode', 'Year'])[
            'EventPlotCount'].sum().reset_index(name='MonitoringCyclePlotCount')

        # Sum the Total Cover By Species By Community Monitoring Cycle
        nawmaMonCycleTotalCover = DFNAWMAwPCwEvent.groupby(['VegCode', 'Year', 'Species'])[
            'PercentCover'].sum().reset_index(
            name='MonitoringCycleTotalCover')

        # Join the Total Cover and Plot Count By Monitoring Cycle Dataframes
        nawmaMonCycleTCwPC = pd.merge(nawmaMonCycleTotalCover, nawmaPlotCountComMonCycle[['VegCode', 'Year',
                                                                                          'MonitoringCyclePlotCount']],
                                      on=['VegCode', 'Year'])

        # Calculate the Community Monitoring Cycle Average Species Cover
        nawmaMonCycleTCwPC['MonitoringCycleAverageCover'] = nawmaMonCycleTCwPC['MonitoringCycleTotalCover'] / \
                                                            nawmaMonCycleTCwPC['MonitoringCyclePlotCount']

        return 'success function', nawmaMonCycleTCwPC

    except:
        print(f'Failed - NAWMA_CoverByEvent')
        exit()


def NAWMA_CoverByCommunity(inDF, taxonRemoveList, DfEvents):
    """
    Create the NAWMA Average Cover by Community - Pulling from tblNAWMADataset.

    :param inDF: dataframe with the tblNAWMADataset dataset file
    :param taxonRemoveList: List with the taxon in field 'Species' to be removed from analysis
    :param DfEvents: Events Dataframe will be used to join event metadata to outputs

    :return: nawmaMonCycleTCwPC: Dataframe with the Community Monitoring Cycle Total Cover, Number of Plots and
    Average Cover fields output
    """
    try:
        # Remove NAWMA Plots - only retaining A, B, C subplots which have 50 hits per
        nawmaDFSetup = inDF[inDF['TransectID'] != 'NAWMA']

        # Removed Non-Herbaceous Taxon
        mask = nawmaDFSetup['Species'].isin(taxonRemoveList)

        # apply the Mask, removing the Non-Herbaceous records
        nawmaDFSetup = nawmaDFSetup[~mask]

        # Calculate the Percent Cover as [HitsInQuadrant] * 2 (50 sample points in the NAWMA subplots.
        nawmaDFSetup['PercentCover'] = nawmaDFSetup['HitsInQuadrat'] * 2

        # Join to Event Dataset to get Community
        # Define Field(s) to do join on
        joinFields = ['EventID']

        # Retain only the fields of interest
        fieldsToRetain = ['UnitCode', 'EventID', 'VegCode', 'VegDescription', 'Species', 'TransectID',
                          'HitsInQuadrat', 'PercentCover']

        # Perform the Merge of nawma
        DFNAWMAwPCwEvent = pd.merge(nawmaDFSetup, DfEvents[['EventID', 'StartDate', 'VegCode', 'VegDescription']],
                                    on=joinFields)

        # Subset to the desired fields
        DFNAWMAwPCwEvent = DFNAWMAwPCwEvent.loc[:, fieldsToRetain]

        # Get Number of Plots By Event  (i.e. A, B, C) by event, the norm will be three
        nawmaPlotCountByEvent = (DFNAWMAwPCwEvent.groupby(['EventID', 'VegCode'])['TransectID'].nunique().
                                 reset_index(name='EventPlotCount'))

        # Get Number of Plot by Community
        nawmaPlotCountCommunity = nawmaPlotCountByEvent.groupby(['VegCode'])[
            'EventPlotCount'].sum().reset_index(name='CommunityPlotCount')

        # Sum the Total Cover By Species By Community Monitoring Cycle
        nawmaCommunityTotalCover = DFNAWMAwPCwEvent.groupby(['VegCode', 'Species'])['PercentCover'].sum().reset_index(
            name='CommunityTotalCover')

        # Join the Total Cover and Plot Count By Monitoring Cycle Dataframes
        nawmaCommunityTCwPC = pd.merge(nawmaCommunityTotalCover, nawmaPlotCountCommunity[['VegCode',
                                        'CommunityPlotCount']], on=['VegCode'])

        # Calculate the Community Average Species Cover
        nawmaCommunityTCwPC['CommunityAverageCover'] = nawmaCommunityTCwPC['CommunityTotalCover'] / \
                                                             nawmaCommunityTCwPC['CommunityPlotCount']

        return 'success function', nawmaCommunityTCwPC

    except:
        print(f'Failed - NAWMA_CoverByCommunity')
        exit()


# Connect to Access DB and perform defined query - return query in a dataframe
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

    # Run Main Code Bloc
    main()
