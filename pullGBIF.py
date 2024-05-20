"""
pullGBIF.py
Script will pull species occurences data from the GBIF https://www.gbif.org/ repository via the GBIF API and the
pygbif client - https://www.gbif.org/tool/OlyoYyRbKCSCkMKIi4oIT/pygbif-gbif-python-client

First lookup the GBIF Taxon Code via the 'Species' code in the species API module
 - https://pygbif.readthedocs.io/en/latest/modules/species.html

Second pull the Taxon Occurrences with spatial locations (Lat/Lon) in the occurrernce API module
- https://pygbif.readthedocs.io/en/latest/modules/occurrence.html

Export identified Taxon in GBIF database to a .csv file

Input:
    speciesLookup - Speices Lookup Table with defined Taxon to identified

Output:
    table with the GB

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
from pygbif import species
from pygbif import occurrences as occ

#File (Excel/CSV) with vegtation taxon to be idenfities
inTable = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\PCM\NAWMACover\PCM_NAWMA_Vegetation_ClimateVA_Testing.xlsx'
#Worksheet to process if the inTable is an excel file
inWorksheet = 'NAWMACoverCommunityTopTwo'
#Field in the Vegetation worksheet that defined the scientific name
lookupField = 'Species'

# Output Name, OutDir, and Workspace
outName = 'PCM_NAWMA_TopTwo_GBIF'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\GBIF'  # Directory Output Location
workspace = f'{outDir}\\workspace'  # Workspace Output Directory
dateNow = datetime.now().strftime('%Y%m%d')
logFileName = f'{workspace}\\{outName}_{dateNow}.LogFile.txt'  # Name of the .txt script logfile which is saved in the workspace directory

#Occurrence Download Parameters
#Number of records to download per GBIF Occurrence Call on occurrence.search
chunkSize = 300
#Total Number of GBIF records to download per
totalRecords = 10000
#List of fiels to retain from the GBIF Occurrence Pulls
fieldsToRetain = ['key', 'taxonKey', 'scientificName', 'basisOfRecord', 'taxonomicStatus', 'year', 'eventDate',
                  'decimalLatitude', 'decimalLongitude', 'continent', 'stateProvince', 'country', 'datasetName',
                  'institutionCode']

def main():
    try:
        session_info.show()

        outDFList = []
        ########################
        #Read in the Taxon Table
        ########################

        inFormat = os.path.splitext(inTable)[1]

        if inFormat == '.xlsx':
            inDF = pd.read_excel(inTable, sheet_name=inWorksheet)

        #########################################################
        #Hit the GBIF Species Module to get the GBIF Taxon Key/ID
        #########################################################

        outFun = processTaxonomy(inDF, lookupField)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function getTaxonomy - " + messageTime + " - Failed - Exiting Script")
            exit()

        DFSpeciesTaxonomy = outFun[1]
        #Export Dataframe with the Defined GBIF Species Fields
        outSpeciesList = f'{outDir}\\{outName}_Species_{dateNow}.csv'
        DFSpeciesTaxonomy.to_csv(outSpeciesList)

        ############################################################
        # Pull Occurrence Data via the PYGBIF API Occurrences.search
        ############################################################

        outFun = processOccurrence(DFSpeciesTaxonomy,  chunkSize, totalRecords, fieldsToRetain)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function processOccurrence - " + messageTime + " - Failed - Exiting Script")
            exit()
        #Output Occurrence Dataframe
        outDFOccurrenceList = outFun[1]

        ####################
        #Export Taxonomy and Occurrence Dataframes.  Occurrence dataframe will be subset as well
        #Option to remove fields
        outPathName = f'{outDir}\\{outName}_Occurrences_{dateNow}.csv'

        # Export to .csv file
        outDFOccurrenceList.to_csv(outPathName)

        messageTime = timeFun()
        scriptMsg = f'Successfully Exported Occurrence Records - {outPathName} - {messageTime}'
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - pullGBIF.py - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()
        traceback.print_exc(file=sys.stdout)

    finally:
        exit()

def processOccurrence(inDF, chunkSize, totalRecords, fieldToRetain):
    """
    Parent Function where processing of the GBIF taxonomies identified in the 'processTaxonomy' routine (i.e. all the
    defined in the input table species list) are pulled from the GBIF occurrence module
    https://pygbif.readthedocs.io/en/latest/modules/occurrence.html

    :param inDF: dataframe with taxonomy records to be pulled with defined GBIF Taxon ID.
    :param chunkSize: Number of records to be pulled in the occurrence API pull
    :param totalRecords: Total Number of records to be pulled by GBIF Taxon Key in the occurrence API pull

    :return: outOccurenceDF: List with dataframes per Taxon Occurrence records pulled from GBIF

    """
    try:

        #List to hold the output dataframes with the occurrence data by Taxon

        outDFOccurrenceList = []

        #Iterate through the inDF
        for index, row in inDF.iterrows():
            taxonLU = row.get('ScientificNameGBIF')
            GBIFKeyLU = row.get('GBIFKey')
            VegCodeLU = row.get('VegCode')
            #Hit the GBIF Species API to pull the GBIF Key value via the 'Scientific Name', using best match, returning
            #the GBIFKey value, confidence and match type attributes, option for other info is desired.
            outFun = getOccurrence(GBIFKeyLU, chunkSize, totalRecords, fieldToRetain, VegCodeLU)
            if outFun[0].lower() != "success function":
                messageTime = timeFun()
                print("WARNING - Function getOccurrence - failed for - " + taxonLU + ' - ' + messageTime + " "
                    " - Exiting Script")
                exit()
            #
            outGBIFOccurrence = outFun[1]
            #Add the Taxon Occudrrence Dataframe to the list to be compiled across all taxon
            outDFOccurrenceList.append(outGBIFOccurrence)
            messageTime = timeFun()
            scriptMsg = f'Successfully ProcessOccurrence Data for - {GBIFKeyLU} - for Taxon - {taxonLU }- {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        #Concate All dataframes in list to one occurrence dataframe
        DFOccurrences = pd.concat(outDFOccurrenceList, ignore_index=True)

        return 'success function', DFOccurrences

    except:
        print(f'Failed - processOccurrence')
        exit()

def getOccurrence(GBIFKey, chunkSize, totalRecords, fieldsToRetain, VegCodeLU):
    """
    Routine hit's the GBIF Occurrence API (https://pygbif.readthedocs.io/en/latest/modules/occurrence.html),
    via the passed GBIF Taxon Key pulled from the processTaxonomy routine. Returns a subset of occurrence information
    defined by the 'fieldTORetain' varible.

    Pulling of Occurrence data is filtered using the following criteria:
    taxonKey = GBIF Key
    hasCoordinates = True
    publishingCountry = US
    decimdalLatitude = between 49 and 26 degrees
    decimalLongitude = between -66 and -125
    years 1990-2024


    Option to return more GBIF Occurrence information if desired see API info:
    https://pygbif.readthedocs.io/en/latest/modules/occurrence.html#pygbif.occurrences.download

    :param GBIFKey: GBIF Taxon Key Identification field pulled from the GBIF Species API
    :param chunkSize: Number of records to be pulled in the occurrence API pull
    :param totalRecords: Total Number of records to be pulled by GBIF Taxon Key in the occurrence API pull
    :param fieldsToRetain: Fields in the Occurrence Records output to be exported (i.e. subset)
    :param VegCodeLU: Lookup Veg Cade to pass to the dataframe

    :return: outGBIFOccurrence: Dataframe with the Occurrences in the GBIF database for the defined GBIFKey values.
    """
    try:

        #List to hold all the chuncked results
        occurrencList = []
        #Use the GBIF pygbif occurrences.search to download by Chunks limited to 300 records per API pull
        for offset in range(0, totalRecords, chunkSize):
            occurrence_data = occ.search(taxonKey=GBIFKey, hasCoordinate=True, publishingCountry='US',
                                         decimdalLatitude='26,49', decimalLongitude='-125,-66', year='1990,2024',
                                         limit=chunkSize, offset=offset)
            #Pull Results Key to Dictionary
            results = occurrence_data['results']
            #If not results break from loop
            if not results:
                print(f"No more records to fetch at offset {offset}. Exiting loop.")
                break

            #Add Results Dictionary to be added to the Taxon List by Chunk of records
            occurrencList.extend(results)
            print(f"Fetched {len(results)} records starting from offset {offset} - GBIFKey - {GBIFKey}")

        outGBIFOccurrence = pd.DataFrame(occurrencList)

        # Subset to the desired fields
        outGBIFOccurrence = outGBIFOccurrence.loc[:, fieldsToRetain]

        # Add 'VegCode' field
        outGBIFOccurrence.insert(2, 'VegCode', VegCodeLU)

        return 'success function', outGBIFOccurrence
    except:
        print(f'Failed - getOccurrence')
        exit()

def processTaxonomy(inDFTaxonomy, lookupField):
    """
    Parent Function where processing of the input table is performed GBIF Taxonomy

    :param inDF: dataframe with taxonomy records to be pulled.
    :param lookupField: field in the inDF dataframe with the taxonomy to be defined/lookup in GBIF

    :return: outTaxonomyDF: Dataframe with the taxonomy records in the 'inDF' with the best match GBIF Taxonomy Key/ID

    Average Cover fields output
    """
    try:


        #Add the GBIF Species fields to dataframe
        inDFTaxonomy['GBIFKey'] = pd.Series([pd.NA] * len(inDFTaxonomy), dtype='Int64')
        inDFTaxonomy['ScientificNameGBIF'] = pd.Series([pd.NA] * len(inDFTaxonomy), dtype='object')
        inDFTaxonomy['Confidence'] = pd.Series([pd.NA] * len(inDFTaxonomy), dtype='Int8')
        inDFTaxonomy['MatchType'] = pd.Series([pd.NA] * len(inDFTaxonomy), dtype='object')

        #Iterate through the inDF
        for index, row in inDFTaxonomy.iterrows():
            taxonLU = row.get(lookupField)

            #Hit the GBIF Species API to pull the GBIF Key value via the 'Scientific Name', using best match, returning
            #the GBIFKey value, confidence and match type attributes, option for other info is desired.
            outFun = getTaxonomy(taxonLU)
            if outFun[0].lower() != "success function":
                messageTime = timeFun()
                print("WARNING - Function getTaxonomy - failed for - " + taxonLU + ' - ' + messageTime + " "
                    " - Exiting Script")
                exit()

            outGBIFSpecies = outFun[1]

            outGBIFKey = outGBIFSpecies['GBIFKey'][0]
            outSciName = outGBIFSpecies['scientificName'][0]
            outConfidence = outGBIFSpecies['confidence'][0]
            outMatchType = outGBIFSpecies['matchType'][0]

            # Update the GBIF Species  fields
            inDFTaxonomy.at[index, 'GBIFKey'] = outGBIFKey
            inDFTaxonomy.at[index, 'ScientificNameGBIF'] = outSciName
            inDFTaxonomy.at[index, 'Confidence'] = outConfidence
            inDFTaxonomy.at[index, 'MatchType'] = outMatchType

            messageTime = timeFun()
            scriptMsg = f'Successfully defined GBIF Key for - {taxonLU} - {messageTime}'
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

        messageTime = timeFun()
        scriptMsg = f'Successfully Completed function processTaxonomy - {messageTime}'
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return 'success function', inDFTaxonomy

    except:
        print(f'Failed - processTaxonomy')
        exit()


def getTaxonomy(taxonLU):
    """
    Routine hit's the GBIF Species API (https://pygbif.readthedocs.io/en/latest/modules/species.html),
    via the passed species scientific name field uses the species.name_backbone service to return the best match GDBIF
    record.  Return the GBIF Key/UsageKey, Confidence in taxonomy match, and the type of match.

    Option to return more GBIF species information if desired

    :param taxonLU: Taxonomic Scientific Name

    :return: outGBIFSpecies: Dataframe with the above Taxonomy GBIF values (GBIF Key, Confidence in match and type of
    match.

    """
    try:

        #Use the GBIF pygbif species.name_backbone() to pull the best match
        outGBIF= species.name_backbone(taxonLU, rank="species", verbose=False)

        #Pull variables of interest
        usageKey = outGBIF['usageKey']
        scientificName = outGBIF['scientificName']
        confidence = outGBIF['confidence']
        matchType = outGBIF['matchType']

        #Create Dictionary:
        workDict = {'GBIFKey': usageKey,
                    'scientificName': scientificName,
                    'confidence': confidence,
                    'matchType': matchType}

        #Output Dataframe - must define an index value for the dataframe to be created
        outGBIFSpecies = pd.DataFrame([workDict], index=['GBIFIndex'])

        return 'success function', outGBIFSpecies
    except:
        print(f'Failed - getTaxonomy')
        exit()

def timeFun():
    try:
        b = datetime.now()
        messageTime = b.isoformat()
        return messageTime
    except:
        print(f'Failed - timeFun')

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