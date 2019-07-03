#need path for sparkR in Path env

library(SparkR)

#install.packages("pillar")
#install.packages("remotes")
#remotes::install_github("r-spark/geospark")

library(geospark)
library(magrittr)


sparkR.session(appName="MigrationPlus")
#sqlContext <- sparkRSQL.init(sc)

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


ghcnd_obs <- read.df("C:/Users/Martin/Documents/Job Search/InsightExercise/InsightProgram/Data/2002Subset.csv",
                   "csv", na.strings = "", schema=ghcnd_obs_schema)

#transpose manually
#split into sub data frames for each element, together so each station+date is one row
ghcnd2 <- ghcnd_obs %>% withColumn("date_processed", to_date(.$date,  "yyyyMMdd")) %>% 
  filter("element in ('PRCP','SNOW','SNWD','TMIN','TMAX')") %>%
  select(c("id","element","element_val","date_processed"))
#Cant subset rows because no row numbers in spark (requres whole dataset)

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
#                   COALESCE(tmin.date_processed, tmax.date_processed, prcp.date_processed, snow.date_processed, snwd.date_processed), 
#                   tmin.tmin, tmax.tmax, prcp.prcp, snow.snow, snwd.snwd
#                   from tmin
#                   FULL OUTER JOIN tmax
#                   ON tmin.id = tmax.id AND tmin.date_processed=tmax.date_processed")


flat_ghcnd2 <- sql("Select COALESCE(tmin.id, tmax.id, prcp.id, snow.id, snwd.id) as id, 
                  COALESCE(tmin.date_processed, tmax.date_processed, prcp.date_processed,
                          snow.date_processed, snwd.date_processed) as date, 
                  tmin.tmin, tmax.tmax, prcp.prcp, snow.snow, snwd.snwd
                  from tmin
                  FULL OUTER JOIN tmax
                  ON tmin.id = tmax.id AND tmin.date_processed=tmax.date_processed
                  FULL OUTER JOIN prcp
                  ON prcp.id=COALESCE(tmin.id, tmax.id)
                     AND prcp.date_processed=COALESCE(tmin.date_processed, tmax.date_processed)
                  FULL OUTER JOIN snow
                  ON snow.id=COALESCE(tmin.id, tmax.id, prcp.id)
                   AND snow.date_processed=COALESCE(tmin.date_processed, tmax.date_processed,
                                                    prcp.date_processed)
                  FULL OUTER JOIN snwd
                  ON snwd.id=COALESCE(tmin.id, tmax.id, prcp.id, snow.id)
                   AND snwd.date_processed=COALESCE(tmin.date_processed, tmax.date_processed,
                   prcp.date_processed, snow.date_processed)")

#show(ghcnd2)
showDF(flat_ghcnd2)

#count(flat_ghcnd2)
#showDF(ghcnd2)


sparkR.session.stop()
sparkR.stop()
