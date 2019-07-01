--Instructions from
--https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS
--https://geekforum.wordpress.com/2015/04/01/setting-up-postgis-on-aws-rds-postgresql-database-server/


select current_user;
create extension postgis;

create extension fuzzystrmatch;

create extension postgis_tiger_geocoder;

create extension postgis_topology;

select * from information_schema.schemata;
alter schema tiger owner to rds_superuser;
alter schema topology owner to rds_superuser;

--alter schema pg_catalog owner to rds_superuser;
--alter schema information_schema owner to rds_superuser;

CREATE FUNCTION exec(text) returns text language plpgsql volatile AS $f$ BEGIN EXECUTE $1; RETURN $1; END; $f$;      

SELECT exec('ALTER TABLE ' || quote_ident(s.nspname) || '.' || quote_ident(s.relname) || ' OWNER TO rds_superuser;')
  FROM (
    SELECT nspname, relname
    FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) 
    WHERE nspname in ('tiger','topology') AND
    relkind IN ('r','S','v') ORDER BY relkind = 'S')
s;  

--SET search_path=public,tiger;    
ALTER DATABASE migrationplus SET search_path=public,tiger; 

--tests to make sure functions are added
--select na.address, na.streetname, na.streettypeabbrev, na.zip
--from normalize_address('1 Devonshire Place, Boston, MA 02109') as na;

--select topology.createtopology('my_new_topo',26986,0.5);
