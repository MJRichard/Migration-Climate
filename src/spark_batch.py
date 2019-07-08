"""
Spark batch process in spark to join animal migration data with daily climate summary.

Data:
Climate summary for weather station id (gzipped)
Weather station ids and locations
Study data

Read in data, transform weather data to one row per station/day
Write result to PostgreSQL
"""

from pyspark.sql import SparkSession, types, DataFrameReader
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, FloatType, IntegerType

spark = SparkSession.builder.appName("Migration").getOrCreate()
assert isinstance(spark.sparkContext, object)


# read in each file, define schema to raise error if file format is different
'''
read in stations datafile
ghcnd-stations.txt schema
ID            1-11   Character
LATITUDE     13-20   Real (decimal degrees)
LONGITUDE    22-30   Real (decimal degrees)
ELEVATION    32-37   Real
STATE        39-40   Character
NAME         42-71   Character
GSN FLAG     73-75   Character
HCN/CRN FLAG 77-79   Character
WMO ID       81-85   Character
'''

def read_stations(s3_location):
    ghcnd_raw = spark.read.text(s3_location)
    ghcnd_stations = ghcnd_raw.select(
        ghcnd_raw.value.substr(1, 11).alias('id'),
        ghcnd_raw.value.substr(13, 8).cast('float').alias('Latitude'),
        ghcnd_raw.value.substr(23, 8).cast('float').alias('Longitude'),
        ghcnd_raw.value.substr(32, 6).cast('float').alias('Elevation')
    )
    return ghcnd_stations
# other columns not needed, so not processing


'''
Daily data sets
    ID = 11 character station identification code. Please see ghcnd-stations section below for an explantation
    YEAR/MONTH/DAY = 8 character date in YYYYMMDD format (e.g. 19860529 = May 29, 1986)
    ELEMENT = 4 character indicator of element type
    DATA VALUE = 5 character data value for ELEMENT
    M-FLAG = 1 character Measurement Flag
    Q-FLAG = 1 character Quality Flag
    S-FLAG = 1 character Source Flag
    OBS-TIME = 4-character time of observation in hour-minute format (i.e. 0700 =7:00 am)
'''

def read_weather_obs(weather_path):
    ghcnd_obs_schema = StructType([
        StructField("id", StringType(), False),
        StructField("date", DateType(), False),
        StructField("element", StringType(), False),
        StructField("element_val", StringType(), False),
        StructField("m_flag", StringType(), True),
        StructField("q_flag", StringType(), True),
        StructField("s_flag", StringType(), True),
        StructField("obs_time", StringType(), True)])
    weather_data = spark.read.csv(weather_path, schema=ghcnd_obs_schema, dateFormat='yyyyMMdd') \
        .select("id", "date", "element", "element_val")
    return weather_data


def read_pigeon_obs(pigeon_s3):
    pigeon_schema = StructType([
        StructField("eventid", StringType(), False),
        StructField("visible", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("longitude", FloatType(), False),
        StructField("latitude", FloatType(), False),
        StructField("gps", IntegerType(), True),
        StructField("ground_speed", FloatType(), True),
        StructField("height_above_sealevel", FloatType(), True),
        StructField("outlier_flag", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("taxon_name", StringType(), True),
        StructField("tag_local_identifier", StringType(), True),
        StructField("individual_local_identifier", StringType(), True),
        StructField("study_name", StringType(), True)])
    pigeon_obs = spark.read.csv(pigeon_s3, schema=pigeon_schema,
                                 timestampFormat='yyyy-MM-dd HH:mm:ss.SSS',
                                 header=True)
    return pigeon_obs


# best practice is to move db location to config file, not in github directly
def write_to_db(df, table_name):
    db_url = 'jdbc:postgresql://migrationplus2.cbyji2jivihq.us-east-1.rds.amazonaws.com:5432/migrationplus'
    propertiesval = {'user': 'migrationplus', 'password': 'migrationplus', 'batchsize': '50000'}
    df.write.jdbc(url=db_url, table=table_name, mode='overwrite', properties=propertiesval)
    return


# read in all data and run
ghcnd_stations = read_stations("s3a://noaa-ghcn-pds/ghcnd-stations.txt")
pigeon_obs = read_pigeon_obs("s3a://insightmovementweather/MigrationData/Pigeon.csv")


weather_df = read_weather_obs("s3a://noaa-ghcn-pds/csv/*.csv")

# transform weather observations into one row that includes the 5 main features
tmin = weather_df.filter(weather_df.element == "TMIN").withColumnRenamed("element_val", "tmin")
tmax = weather_df.filter(weather_df.element == "TMAX").withColumnRenamed("element_val", "tmax")
prcp = weather_df.filter(weather_df.element == "PRCP").withColumnRenamed("element_val", "prcp")
snow = weather_df.filter(weather_df.element == "SNOW").withColumnRenamed("element_val", "snow")
snwd = weather_df.filter(weather_df.element == "SNWD").withColumnRenamed("element_val", "snwd")

tmin.createOrReplaceTempView("tmin")
tmax.createOrReplaceTempView("tmax")
prcp.createOrReplaceTempView("prcp")
snow.createOrReplaceTempView("snow")
snwd.createOrReplaceTempView("snwd")

# join for one observation per row
flat_ghcnd = spark.sql("""Select COALESCE(tmin.id, tmax.id, prcp.id, snow.id, snwd.id) as id,
                  COALESCE(tmin.date, tmax.date, prcp.date,
                          snow.date, snwd.date) as date,
                  tmin.tmin, tmax.tmax, prcp.prcp, snow.snow, snwd.snwd
                  from tmin
                  FULL OUTER JOIN tmax
                  ON tmin.id = tmax.id AND tmin.date=tmax.date
                  FULL OUTER JOIN prcp
                  ON prcp.id=COALESCE(tmin.id, tmax.id)
                     AND prcp.date=COALESCE(tmin.date, tmax.date)
                  FULL OUTER JOIN snow
                  ON snow.id=COALESCE(tmin.id, tmax.id, prcp.id)
                   AND snow.date=COALESCE(tmin.date, tmax.date,
                                                    prcp.date)
                  FULL OUTER JOIN snwd
                  ON snwd.id=COALESCE(tmin.id, tmax.id, prcp.id, snow.id)
                   AND snwd.date=COALESCE(tmin.date, tmax.date,
                   prcp.date, snow.date) """)

#write all 3 to database
write_to_db(pigeon_obs, 'pidgeon_sensor')
write_to_db(flat_ghcnd, 'station_flatobs')
write_to_db(ghcnd_stations, 'stations')

