ALTER TABLE pidgeon_sensor
  ADD sensor_loc GEOGRAPHY(POINT);

ALTER TABLE stations
  ADD station_loc GEOGRAPHY(POINT);
  
ALTER TABLE pidgeon_sensor
  ADD date date;
--add date column for indexing
--move this over to spark ETL to run faster
  

-- 1m48 for 8million rows 
UPDATE pidgeon_sensor
  SET sensor_loc = ST_MakePoint(longitude, latitude);

UPDATE stations
  SET station_loc = ST_MakePoint("Longitude", "Latitude");
-- 0.42s for 113933 rows, needs to be capital since written from spark as capital...

UPDATE pidgeon_sensor
  SET date = date(timestamp); 



CREATE INDEX stations_gix ON stations using GIST(station_loc);
CREATE INDEX pidgeon_sensor_gix ON pidgeon_sensor using GIST(sensor_loc);
--1sec and 2.40 minutes to create;

CREATE INDEX station_obs_id ON station_flatobs(id);
CREATE INDEX ix_station_obs_date ON station_flatobs(date);

CREATE INDEX ix_stations_id ON stations(id);

CREATE INDEX ix_pidgeon_date ON pidgeon_sensor(date);

VACUUM ANALYZE stations;
VACUUM ANALYZE pidgeon_sensor;
VACUUM ANALYZE station_flatobs;


--EXPLAIN
CREATE TABLE closest_stations
AS
select ps_join.eventid, ps_join.id, ps_join.date, ps_join.station_p_obs_distance from
(select 
       p.eventid, p.timestamp, p.latitude as p_lat, p.longitude as p_long, 
       p.tag_local_identifier, p.sensor_loc as pid_loc,
       s.id, s."Latitude" as s_lat, s."Longitude" as s_long, sl.date, sl.tmin, sl.tmax, sl.prcp,
       ST_Distance(p.sensor_loc, s.station_loc) as station_p_obs_distance
from pidgeon_sensor p,
     station_flatobs sl,
     stations s
where p.date=sl.date
      AND sl.id=s.id
      AND ((sl.tmin IS NOT NULL) OR (sl.tmax IS NOT NULL))
      --AND p.sensor_loc <> s.station_loc
      AND ST_DWithin(p.sensor_loc, s.station_loc, 25000) --25km
) ps_join
;
--16 minutes to creation for 16000
--28 min for 25000,37 million rows, ~8.728 rows

CREATE OR REPLACE VIEW V_closest_stations_obs AS
select p.*, s.id, s.tmin, s.tmax, s.prcp, s.snow, s.snwd
from
  (select * 
  from closest_stations
  where (eventid,station_p_obs_distance) in (select eventid, min(station_p_obs_distance) from closest_stations group by eventid)
  ) cs
join pidgeon_sensor p 
  on cs.eventid=p.eventid
join station_flatobs s
  on cs.id=s.id and cs.date=s.date;

CREATE OR REPLACE VIEW V_weighted_mean_obs AS
select p.*, wm.tmin, wm.tmax
from
(select cs.eventid, sum(CAST(s.tmin AS NUMERIC) / cs.station_p_obs_distance)/sum(case when s.tmin is not null then 1 / cs.station_p_obs_distance else 0 end) as tmin,
                   sum(CAST(s.tmax AS NUMERIC) / cs.station_p_obs_distance)/sum(case when s.tmax is not null then 1 / cs.station_p_obs_distance else 0 end) as tmax
from closest_stations cs
join station_flatobs s
  on cs.id=s.id and cs.date=s.date
group by cs.eventid) wm
join pidgeon_sensor p
on p.eventid=wm.eventid;

select * from v_weighted_mean_obs
limit 10;

select * from v_closest_stations_obs
limit 10;
 
select p.*, s.tmin, s.tmax
from pidgeon_sensor p
join (select eventid,  from closest_stations
      group by eventid

closest_stations c
  on p.eventid=c.;
explain  
select * from 
(select row_number() over (partition by eventid ORDER BY eventid, station_p_obs_distance) as rownum
  from closest_stations) c
where rownum=1; 

--explain
select * from closest_stations
where (eventid,station_p_obs_distance) in (select eventid, min(station_p_obs_distance) from closest_stations group by eventid)
limit 100;

explain
select p.*, s.*, c.station_p_obs_distance
from (select * from closest_stations limit 1000) c
join pidgeon_sensor p 
  on c.eventid=p.eventid
join station_flatobs s
  on c.id=s.id and c.date=s.date;

-- select eventid, count(*)
-- from closest_stations
-- group by eventid
-- having count(*)>1
-- limit 500;

select reltuples as approxcount
from pg_class where relname = 'closest_stations';
--38million (avg ~5 stations per obs)

-- select count(distinct eventid)
-- from closest_stations;
--8.59 million
-- 
-- select count(*) from pidgeon_sensor;
--8.72million



select * from sensor_station_distance;

select * from information_schema.columns
where table_name like 'sta%';

select count(*) from pidgeon_sensor;
--8.7 million
select count(*) from stations;
select count(*) from station_flatobs;
--1billion
SELECT reltuples AS approximate_row_count FROM pg_class
where relname = 'station_flatobs';

select * from station_flatobs where snow is not null order by id, date limit 100;
select * from station_flatobs limit 100;

select * from pidgeon_sensor limit 100;

select * from station_obs limit 100;
select * from stations limit 100;
select * from closest_stations limit 100;
-- DROP TABLE station;
-- DELETE FROM pidgeon_sensor;
-- DELETE FROM station_obs;
-- DELETE FROM stations;
-- DELETE FROM closest_stations;
DROP TABLE closest_stations;


select * from pg_indexes where schemaname='public';

--SELECT * FROM pg_stat_activity;
--SELECT * FROM pg_stat_user_activity;

select date(timestamp)
from pidgeon_sensor
limit 100;

select cast(timestamp as date) as p_date
FROM pidgeon_sensor
limit 50;


select a.eventid, count(*) 
from
(select --DISTINCT ON(p.eventid) 
       p.eventid, p.timestamp, p.latitude as p_lat, p.longitude as p_long, 
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
limit 100000) a
group by a.eventid
having count(*)>3
;
--14s, 14000 rows


select column_name, data_type
from information_schema.columns
where table_name = 'station_flatobs';

select column_name, data_type
from information_schema.columns
where table_name = 'station_obs';
