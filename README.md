# MigrationPlus
MigrationPlus is a data pipeline for researchers studying animal migrations to join their data
 from tracking sensors with the weather conditions on the day of observation. 
This project was developed while being a Data Engineering Fellow at Insight Data Science
in Boston session 19B.

## Pipeline
![pipeline](img/pipeline.jpg)

## Data Sources

### NOAA Daily Observations
The weather data is from the [NOAA Global Historical Climatology Network Daily (GHCN-D)]
(https://registry.opendata.aws/noaa-ghcn/).
It is a collection of daily weather observations from stations across the world, with the 
earliest data from 1763 and going until the present. 
The 5 main observation elements that are reported are 
precipitation, snowfall, snow depth, maximum temperature, and minimum temperature. 
The data is already available in S3 as part of the "Registry of Open Data on AWS" 
and can be accessed directly. 
The files are grouped into a file for each year, available both as a csv, or a gzipped file.
Spark read dataFrame can read either one, but gzip files are non-splittable and so must
be uncompressed on a single node. I will be accessing the files as csv for processing efficiency.

### Movebank
[Movebank](https://www.movebank.org/) is a repository for researchers to add their animal tracking data.
Some datasets are publicly available, but some private data can be visualized.
The [Movebank Data Repository](https://www.datarepository.movebank.org/) is a repository
for published datasets that are publicly available.
In January 2019, the repository contained 141 data packages and 369 data files.
The study on the [behavioral traits for homing pidgeons](https://www.datarepository.movebank.org/handle/10255/move.766)
 was selected as the datasource to test MigrationPlus, as it was one of the largest datasets available
 and the data was collected over a range of years. 
The datafile is downloaded and added to an S3 bucket.

## Installation

## Repo Structure

