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
