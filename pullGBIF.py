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
    speciesLookup - Speices Lookup Table with defined Taxon to be pulled from the

Output:


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

#File (Excel/CSV) with vegtation taxon to be idenfities
inTable = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\Climate\VulnerabilityAssessment\PCM\NAWMACover\PCM_NAWMA_Vegetation_ClimateVA_20240516.xlsx'
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


def main():
    try:
        session_info.show()

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

        #########################################################
        # Pull Occurrence Data
        #########################################################

        #STOPPED HERE 5/17/2024 - KRS
        outFun = processOccurrence(inDF, lookupField)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function processOccurrence - " + messageTime + " - Failed - Exiting Script")
            exit()






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


def processTaxonomy(inDF, lookupField):
    """
    Parent Function where processing of the input table is performed GBIF Taxonomy

    :param inDF: dataframe with taxonomy records to be pulled.
    :param lookupField: field in the inDF dataframe with the taxonomy to be defined/lookup in GBIF

    :return: outTaxonomyDF: Dataframe with the taxonomy records in the 'inDF' with the best match GBIF Taxonomy Key/ID

    Average Cover fields output
    """
    try:

        #Add the GBIF Species fields to dataframe
        inDF['GBIFKey'] = pd.Series([pd.NA] * len(inDF), dtype='Int64')
        inDF['ScientificNameGBIF'] = pd.Series([pd.NA] * len(inDF), dtype='object')
        inDF['Confidence'] = pd.Series([pd.NA] * len(inDF), dtype='Int8')
        inDF['MatchType'] = pd.Series([pd.NA] * len(inDF), dtype='object')

        #Iterate through the inDF
        for index, row in inDF.iterrows():
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
            inDF.at[index, 'GBIFKey'] = outGBIFKey
            inDF.at[index, 'ScientificNameGBIF'] = outSciName
            inDF.at[index, 'Confidence'] = outConfidence
            inDF.at[index, 'MatchType'] = outMatchType

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

        return 'success function', inDF

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