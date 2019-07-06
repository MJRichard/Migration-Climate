'''
Batch process in spark to join animal migration data with daily climate summary.

Data:
Climate summary for weather station id (gzipped)
Weather station ids and locations
Study data

Join data based on same day, filter to nearest location
Write result to PostgreSQL
'''

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession, types, DataFrameReader
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, FloatType, IntegerType
from pyspark.sql.functions import udf, struct, col
from math import radians, sin, cos, sqrt, asin
import math
import datetime

#attempts to get spark config working
#sqlContext = SQLContext(sc)
#spark_conf = SparkConf().setAppName("WeatherStations")

#sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName("Migration").getOrCreate()
assert isinstance(spark.sparkContext, object)
#sc = spark.sparkContext

'''
ghcnd-stations.txt format
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

#print("*************\n ************\n **********\n start  time: "+ datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")+"\n************\n ***************\n *************" )


#s3 bucket http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt
#ghcnd_raw = spark.read.text("../../Data/ghcnd-stations.txt").limit(1000)
ghcnd_raw = spark.read.text("s3a://noaa-ghcn-pds/ghcnd-stations.txt")

ghcnd_df=ghcnd_raw.select(
    ghcnd_raw.value.substr(1,11).alias('id'),
    ghcnd_raw.value.substr(13,8).cast('float').alias('Latitude'),
    ghcnd_raw.value.substr(23,8).cast('float').alias('Longitude'),
    ghcnd_raw.value.substr(32,6).cast('float').alias('Elevation')
)
#other columns not needed, so not processing

#ghcnd_df.show()
#float drops precision 0s

#print("*************\n ************\n **********\n ghcnd read in" +str(ghcnd_df.count()) + " time: "+ datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")+"\n************\n ***************\n *************" )
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

#define schema since there is no header
ghcnd_obs_schema = StructType([
    StructField("id", StringType(), False),
    StructField("date", DateType(),False),
    StructField("element",StringType(),False),
    StructField("element_val",StringType(),False),
    StructField("m_flag",StringType(),True),
    StructField("q_flag",StringType(),True),
    StructField("s_flag",StringType(),True),
    StructField("obs_time",StringType(),True)])
#raw data for obs_time in subset data in format 070.0 instead of 0700 due to read/write issue when subsetting


#Read in weather observations
# AWS Bucket http://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/1788.csv.gz
#noaa-ghcn-pds.s3.amazonaws.com/csv.gz/1788.csv.gz
#weather_data = spark.read.csv("../../Data/2002subset.csv", schema=ghcnd_obs_schema, dateFormat='yyyyMMdd')

weather_data = spark.read.csv("s3a://noaa-ghcn-pds/csv/2002.csv", schema=ghcnd_obs_schema, dateFormat='yyyyMMdd')\
                    .select("id", "date", "element", "element_val")
#weather_data.show()

tmin = weather_data.filter(weather_data.element == "TMIN").withColumnRenamed("element_val", "tmin")
tmax = weather_data.filter(weather_data.element == "TMAX").withColumnRenamed("element_val", "tmax")
prcp = weather_data.filter(weather_data.element == "PRCP").withColumnRenamed("element_val", "prcp")
snow = weather_data.filter(weather_data.element == "SNOW").withColumnRenamed("element_val", "snow")
snwd = weather_data.filter(weather_data.element == "SNWD").withColumnRenamed("element_val", "snwd")

tmin.createOrReplaceTempView("tmin")
tmax.createOrReplaceTempView("tmax")
prcp.createOrReplaceTempView("prcp")
snow.createOrReplaceTempView("snow")
snwd.createOrReplaceTempView("snwd")

flat_ghcnd2 = spark.sql("""Select COALESCE(tmin.id, tmax.id, prcp.id, snow.id, snwd.id) as id,
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
                   prcp.date, snow.date)""")
#print("*************\n ************\n **********\n 2002 weather read in done" +str(weather_data.count()) + " time: "+ datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")+"\n************\n ***************\n *************" )


#change date to unified format

pidgeon_schema = StructType([
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

#pidgeon_obs = spark.read.csv("../../Data/Pigeonsubset.csv", schema=pidgeon_schema, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS',
#                             header=True).limit(100)
pidgeon_obs = spark.read.csv("s3a://insightmovementweather/MigrationData/Pigeon.csv", schema=pidgeon_schema, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS',
                             header=True)


#print("*************\n ************\n **********\n pidgeon read in done" +str(pidgeon_obs.count()) + " time: "+ datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")+"\n************\n ***************\n *************" )


#pidgeon_obs = pidgeon_obs.withColumn('date', pidgeon_obs['timestamp'].cast('date'))
#pidgeon_obs.show()
#pidgeon_obs.printSchema()

#join stations and observations to then calculate distance between every station and observation

'''
station_obs_join = pidgeon_obs.select("eventid", "latitude", "longitude").withColumnRenamed("latitude","obs_lat").withColumnRenamed("longitude","obs_long")\
    .crossJoin(ghcnd_df.select("id","Latitude","Longitude").withColumnRenamed("Latitude","station_lat").withColumnRenamed("Longitude","station_long"))\
    .select("eventid","id","obs_lat","obs_long","station_lat","station_long")

#station_obs_join.count()
'''

def haversine_distance(lat1,lon1,lat2,lon2):
    '''
    Calculates Haversine distance between two lat/lon coordinates
    array in format [lat1, lon1, lat2, lon2]
    :param lat1: Latitude of first point
    :param lon1: Longitude of first point
    :param lat2: Latitude of second point
    :param lon2: Longitude of second point
    :returns:    Float, distance between two points in km
    '''
    R = 6372.8 # Earth radius in kilometers

    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(delta_lat / 2.0) ** 2 + cos(lat1) * cos(lat2) * sin(delta_lon / 2.0) ** 2
    c = 2 * asin(sqrt(a))

    return R * c

udf_func = udf(lambda a,b,c,d: haversine_distance(a,b,c,d),returnType=FloatType())


'''
station_obs_calc = station_obs_join.withColumn('dist',udf_func(station_obs_join['obs_lat'],station_obs_join['obs_long'],station_obs_join['station_lat'],station_obs_join['station_long']))
'''

#station_obs_calc=station_obs_join.withColumn("dist", haversine_distance("obs_lat", "obs_long", "station_lat", "station_long"))
#station_obs_calc.show()
#calculate haversine distance
#station_output=station_obs_calc.select(col("eventid"),col("id"), col("dist").alias("distance"))

#station_obs_calc.write.csv("s3a://insightmovementweather/output_data/testjoin.csv")

urlval = 'jdbc:postgresql://ec2-34-195-21-119.compute-1.amazonaws.com:5432/migrationplus'
urlval2 = 'jdbc:postgresql://migrationplus2.cbyji2jivihq.us-east-1.rds.amazonaws.com:5432/migrationplus'

propertiesval = {'user': 'migrationplus', 'password': 'migrationplus', 'batchsize': '50000'}
#station_output.write.jdbc(url=urlval2, table='sensor_station_distance', mode='append', properties=propertiesval)

pidgeon_obs.write.jdbc(url=urlval2, table='pidgeon_sensor', mode='overwrite', properties=propertiesval)

#print("*************\n ************\n **********\n pidgeon write in??" +str(ghcnd_df.count()) + " time: "+ datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")+"\n************\n ***************\n *************" )

#print(type(weather_data))
#print("COUNT", weather_data.count())
#weather_data.write.jdbc(url=urlval2, table='station_obs', mode='overwrite', properties=propertiesval)

flat_ghcnd2.write.jdbc(url=urlval2, table='station_flatobs', mode='overwrite', properties=propertiesval)
#print("*************\n ************\n **********\nweather obs write in??" +str(ghcnd_df.count()) + " time: "+ datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")+"\n************\n ***************\n *************" )


ghcnd_df.write.jdbc(url=urlval2, table='stations', mode='overwrite', properties=propertiesval)

