--database tables
  
CREATE TABLE pidgeon_sensor(
  eventid VARCHAR (20) PRIMARY KEY,
  visible VARCHAR (50),
  timestamp TIMESTAMP,
  longitude NUMERIC(9,5),
  latitude NUMERIC(9,5),
  gps INT,
  ground_speed FLOAT,
  height_above_sealevel NUMERIC(8,2),
  outlier_flag VARCHAR(5),
  sensor_type VARCHAR(10),
  taxon_name VARCHAR(50),
  tag_local_identifier VARCHAR(20),
  individual_local_identifier VARCHAR(20),
  study_name VARCHAR(100)
--  sensor_loc GEOGRAPHY(POINT)
);

CREATE TABLE station_obs(
  id VARCHAR (11) NOT NULL,
  date DATE NOT NULL,
  element VARCHAR(4),
  element_val VARCHAR(10),
  m_flag VARCHAR(1),
  q_flag VARCHAR(1),  
  s_flag VARCHAR(1),
  obs_time VARCHAR(4)
);
--replace with flatobs tables

CREATE TABLE stations(
  id VARCHAR (11) NOT NULL,
  longitude NUMERIC(8,5) NOT NULL,
  latitude NUMERIC(8,5) NOT NULL,
  elevation NUMERIC(8,2)
--  station_loc GEOGRAPHY(POINT)
);
