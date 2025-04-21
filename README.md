# PCM_ClimateVA

 Plant Communities Monitoring Climate Vulnearability Analysis Bioclimatic Envelope Scripts

 ## PCM_VegSummaries_ClimateVA.py
 Summarizes Plant Communities Monitoring NAWMA vegetation data

 ## pullGBIF.py
Performs a taxonomy cross walk by Scientific Name to the GBIF Taxon Key and then pulls Taxon Occurrences from the [GBIF](https://www.gbif.org/) platform.
First lookups the GBIF Taxon Code via the 'Species' code in the species API module
 - https://pygbif.readthedocs.io/en/latest/modules/species.html
Second pulls the Taxon Occurrences with spatial locations (Lat/Lon) in the occurrence API module
- https://pygbif.readthedocs.io/en/latest/modules/occurrence.html
Export identified Taxon in GBIF database to a .csv file

 ## extractAETDeficit.py
Script extracts point values for the defined point locations (e.g. Monitoring Sites, GBIF occurrences, etc.) and
for the defined rasters (e.g. NPS Water Balance Data). Processing assumes spatial coordinates in the point locations
and rasters are in the same projection (e.g. GCS WGS 84).

Raster point extraction is accomplished using the Rasterio (https://rasterio.readthedocs.io/en/stable/) package.

## graphAETDeficitPCM.py
For extracted AET, and Deficit data creates summary graphs
By Vegetation Type (Iterate):
1) Graphs AET/Deficit Points: Historic, Futures, and Historic and Futures, by PCM community. Outfolder: 'points'
2) Graphs AET/Deficit Vectors change from historic (i.e. 1981-2010) to futures (i.e. Mid Century 2040-2069) PCM and GBIF
by PCM community. Out Folder 'vector'
3) Graphs of Vector change from historic (i.e. 1981-2010) to futures (i.e. Mid Century 2040-2069) PCM only all
communities on one graph. Out Folder 'vector'
4) Graphs AET/Deficit Vectors change from historic (i.e. 1981-2010) to futures (i.e. Mid Century 2040-2069) PCM only,
with GBIF Historic Points plots, by PCM Community. Out Folder 'VectorPCM_GBIFHistoricPts'
