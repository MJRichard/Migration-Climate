select column_name, data_type from information_schema.columns
where table_name = s

select * from sensor_station_distance;

select * from information_schema.columns
where table_name like 'sta%';

select count(*) from stations;
select * from pidgeon_sensor limit 100;

select * from station_obs limit 100;
select * from stations limit 100;

DROP TABLE station;
DELETE FROM pidgeon_sensor;
DELETE FROM station_obs;
DELETE FROM stations;

-- 3m46s run time for 8million rows (148 on new cluster)
UPDATE pidgeon_sensor
  SET sensor_loc = ST_MakePoint(longitude, latitude);


UPDATE stations
  SET station_loc = ST_MakePoint("Longitude", "Latitude");
-- 0.42s for 113933 rows, needs to be capital since written from spark as capital...

CREATE INDEX stations_gix ON stations using GIST(station_loc);
CREATE INDEX pidgeon_sensor_gix ON pidgeon_sensor using GIST(sensor_loc);
--1 and 2 minutes to create;

CREATE INDEX station_obs_date ON station_obs(date);

VACUUM ANALYZE station_obs;
VACUUM ANALYZE stations;
VACUUM ANALYZE pidgeon_sensor;
--fast query;



SELECT *
FROM
    pg_indexes;
--WHERE
--    schemaname = 'public';

SELECT  from pidgeon_sensor
limit 100;

Select p.eventid, p.timestamp, s.id, ST_Distance(p.sensor_loc, s.station_loc)
  FROM (SELECT * FROM pidgeon_sensor LIMIT 100) p,
       stations s
  WHERE ST_DWITHIN(s.station_loc, p.sensor_loc, 10000)
  ORDER BY p.sensor_loc <-> s.station_loc
  LIMIT 200
;

--SELECT * FROM pg_stat_activity;
--SELECT * FROM pg_stat_user_activity;
select distinct id, extract(year from date) as obs_year
from station_obs;

select distinct on(id, date) id, date 
from station_obs
limit 1000;

select date(timestamp)
from pidgeon_sensor
limit 100;

select cast(timestamp as date) as p_date
FROM pidgeon_sensor
limit 50;

select DISTINCT ON(p.eventid) p.eventid, p.timestamp, p.latitude as p_lat, p.longitude as p_long, 
       p.tag_local_identifier, p.sensor_loc as pid_loc,
       s.id, s."Latitude" as s_lat, s."Longitude" as s_long, sl.date, sl.element, sl.element_val,
       ST_Distance(p.sensor_loc, s.station_loc) as station_p_obs_distance
from pidgeon_sensor p,
     station_obs sl,
     stations s
where p.tag_local_identifier = '00-624' 
      AND 
      cast(p.timestamp as date)=sl.date
      AND sl.id=s.id
      AND sl.element='TMIN'
      --AND extract (year from p.timestamp)=2002
      AND p.sensor_loc <> s.station_loc
      AND ST_DWithin(p.sensor_loc, s.station_loc, 16000) --10km
ORDER BY p.eventid, ST_DISTANCE(p.sensor_loc, s.station_loc)
limit 100000
;
--14s, 14000 rows

select --date_trunc(p.timestamp, year)
extract (year from p.timestamp)
from pidgeon_sensor p
limit 10;
