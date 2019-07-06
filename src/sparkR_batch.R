#need path for sparkR in Path env



#install.packages("pillar")
#install.packages("remotes")
#remotes::install_github("r-spark/geospark")
#harryprince/geospark more upto date

library(SparkR)
library(sparklyr)
library(geospark)
library(magrittr)



#conf$spark.serializer <- "org.apache.spark.serializer.KryoSerializer"
#conf$spark.kryo.registrator <- "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator"

sc<-sparkR.session(appName="MigrationPlus")
#register_gis(sc)


ghcnd_obs_schema <- structType(
  structField("id", "string", FALSE),
  structField("date", "string", FALSE),
  structField("element", "string", FALSE),
  structField("element_val", "string", FALSE),
  structField("m_flag", "string", TRUE),
  structField("q_flag", "string", TRUE),
  structField("s_flag", "string", TRUE),
  structField("obs_time", "string", TRUE))
#cant find way to read in date with specific format unless it is default R date format so transform later
#element_val numeric?


ghcnd_obs <- read.df("C:/Users/Martin/Documents/Job Search/InsightExercise/InsightProgram/Data/2002subset.csv",
                   "csv", na.strings = "", schema=ghcnd_obs_schema)

#transpose manually
#split into sub data frames for each element, together so each station+date is one row
ghcnd2 <- ghcnd_obs %>% withColumn("date", to_date(.$date,  "yyyyMMdd")) %>% 
  filter("element in ('PRCP','SNOW','SNWD','TMIN','TMAX')") %>%
  select(c("id","element","element_val","date"))
#Cant subset rows because no row numbers in spark (requres whole dataset)

#count(ghcnd_obs)

tmin <- ghcnd2 %>% filter ("element = 'TMIN'") %>%
  withColumnRenamed("element_val", "tmin")
createOrReplaceTempView(tmin, "tmin")

tmax <- ghcnd2 %>% filter ("element = 'TMAX'") %>%
  withColumnRenamed("element_val", "tmax")
createOrReplaceTempView(tmax, "tmax")
    
prcp <- ghcnd2 %>% filter ("element = 'PRCP'") %>%
  withColumnRenamed("element_val", "prcp")
createOrReplaceTempView(prcp, "prcp")
  
snow <- ghcnd2 %>% filter ("element = 'SNOW'") %>%
  withColumnRenamed("element_val", "snow")
createOrReplaceTempView(snow, "snow")
  
snwd <- ghcnd2 %>% filter ("element = 'SNWD'") %>%
  withColumnRenamed("element_val", "snwd")
createOrReplaceTempView(snwd, "snwd")

# flat_ghcnd <- sql("Select COALESCE(tmin.id, tmax.id, prcp.id, snow.id, snwd.id), 
#                   COALESCE(tmin.date, tmax.date, prcp.date, snow.date, snwd.date), 
#                   tmin.tmin, tmax.tmax, prcp.prcp, snow.snow, snwd.snwd
#                   from tmin
#                   FULL OUTER JOIN tmax
#                   ON tmin.id = tmax.id AND tmin.date=tmax.date")


flat_ghcnd2 <- sql("Select COALESCE(tmin.id, tmax.id, prcp.id, snow.id, snwd.id) as id, 
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
                   prcp.date, snow.date)")

#show(ghcnd2)
#showDF(flat_ghcnd2)

stations_raw <- read.text("C:/Users/Martin/Documents/Job Search/InsightExercise/InsightProgram/Data/ghcnd-stations.txt")

stations_columns <- stations_raw %>%
  withColumn("id", substr(.$value,1,11)) %>%
  withColumn("latitude", substr(.$value,13,20)) %>%
  withColumn("longitude", substr(.$value,22,30)) %>%
  withColumn("elevation", substr(.$value,32,37)) %>%
  drop(.$value)
                            

#count(flat_ghcnd2)
#showDF(stations_columns)

#pidgeon movement dataset
pidgeon_schema <- structType(
  structField("eventid", "string", FALSE),
  structField("visible", "string",TRUE),
  structField("timestamp","string",FALSE),
  structField("longitude","double",FALSE),
  structField("latitude","double",FALSE),
  structField("gps","double",TRUE),
  #int actually for non subseet
  structField("ground_speed","double",TRUE),
  structField("height_above_sealevel","double",TRUE),
  structField("outlier_flag","string",TRUE),
  structField("sensor_type","string",TRUE),
  structField("taxon_name","string",TRUE),
  structField("tag_local_identifier","string",TRUE),
  structField("individual_local_identifier","string",TRUE),
  structField("study_name","string",TRUE))

# pidgeon_obs = spark.read.csv("s3a://insightmovementweather/MigrationData/Pigeon.csv", schema=pidgeon_schema, timestampFormat='yyyy-MM-dd HH:mm:ss.SSS',
#                              header=True)

pidgeon_obs <- read.df("C:/Users/Martin/Documents/Job Search/InsightExercise/InsightProgram/Data/Pigeonsubset.csv",
                     "csv", na.strings = "",header=TRUE, schema=pidgeon_schema)

pidgeon_types<-pidgeon_obs %>%
   withColumn("timestamp", to_timestamp(.$timestamp,'yyyy-MM-dd HH:mm:ss.SSS'))

#printSchema(pidgeon_types)
#showDF(pidgeon_types)
#count(pidgeon_obs)

#write.jdbc(df, "jdbc:postgresql:dbserver", "schema.tablename", user = "username", password = "password")

createOrReplaceTempView(pidgeon_types, "pidgeon_types")

createOrReplaceTempView(stations_columns, "stations_columns")
obs_stations <- sql("SELECT p.event_id, p.longitude, p.latitude, s.longitude, s.latitude
                    FROM pidgeon_types p,
                    stations_columns s
                    WHERE ST_Distance(ST_Point(p.longitude, p.latitude), 
                                      ST_Point(s.longitude, s.latitude))<6000")

showDF(obs_stations)

sparkR.session.stop()
sparkR.stop()
