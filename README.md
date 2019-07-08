# MigrationPlus

MigrationPlus is a data pipeline for researchers studying animal migrations to join their data
 from tracking sensors with the weather conditions on the day of observation. 
Sensors are limited in the amount of information they can collect so additional factors
need to be joined in after data collection.

This project was developed while being a Data Engineering Fellow at Insight Data Science in Boston.

## Pipeline

The daily weather observations are already stored as an AWS Open Data Set in an S3 bucket.
The Movebank migration data file is downloaded into the user's S3 bucket.
In Spark, the data is read in. The weather data is filtered to 5 main attributes,
low temperature (TMIN), high temperature (TMAX), precipitation (PRCP),
snowfall (SNOW) and snow depth (SNWD).
The weather observations are transformed so that there is one row per station per day
instead of a separate row for each weather element. 
The three data tables are loaded into the PostgreSQL database using a JDBC connector.

The PostGIS extension is installed in the database for use of spatial analysis.
This allows only checking distances between points that are within a radius instead
of every combination of weather station and movement sensor.
Point data types are created and indexed for the tables with coordinates and indexed with 
a generalized seatch tree (GIST).
The results output is queried from the database.

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

There is a separate text file ghcnd-stations.txt, which lists each station and it's location,
as well as other identifying information.

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

## Environment Setup

Have an AWS account.

Create an S3 bucket and copy the movebank datassource into it.

The spark cluster consists for 4 m4.large EC2 instances, 1 master and 3 workers.
[Pegasus](https://github.com/InsightDataScience/pegasus) is a useful tool to create
instances by creating a template file in the yaml format.

Download [Postgres JDBC driver 42.2.5](https://jdbc.postgresql.org/download.html) 
jar to each spark instance in the same directory.
On the master node, add the jar directory to spark-defaults.conf on the rows for 
spark.executor.extraClassPath and spark.driver.extraClassPath.
Clone this repository to the master node.
```
git clone https://github.com/MJRichard/Migration-Climate.git
```

The PostgreSQL database is version 10.6 an AWS RDS db.m4.xlarge instance with 
250 GiB of storage attached.
For an RDS database, the PostGIS extension is already installed, but must be loaded into the database.
Instructions are [here](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS)
and also in the PostgreSQL directory of this repository.

Set up the tables in the database, then run the spark job. 
After the data is loaded into the database, run the additional sql scripts to 
create the indicies and join the output.

## Repository Files

```bash
├── img                            Image files for the README
├── postgreSQL
│   │── create_tables.sql          SQL commands to create tables for data load
│   │── post_load_processing.sql   Index and joins for final output
│   └── rds_add_postgis.sql        Steps to load PostGIS functions for an RDS database instance
├── src
│   │── spark_batch.py             Spark job to for data transformation and database load
```
