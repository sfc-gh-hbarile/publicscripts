-- set the context
use role dba_citibike;
create warehouse if not exists demo_wh with warehouse_size = 'small' auto_suspend = 300 initially_suspended = true;
use warehouse demo_wh;


/*********************************************************************
  Create the security integration to S3
  Note - keep it commented out because if you re-create it you need
  to go back into AWS and update the trust relationship...
*********************************************************************/

// create or replace storage integration citibike_snowpipe
//   type = external_stage
//   storage_provider = s3
//   enabled = true
//   storage_aws_role_arn = 'arn:aws:iam::999999999999:role/citibike-snowpipe'
//   storage_allowed_locations = ('s3://citibike-snowpipe');

desc integration citibike_snowpipe;



/*********************************************************************
  Create the schemas and the utility sprocs
*********************************************************************/

-- create the streaming schemas
create or replace schema citibike.raw;
create or replace schema citibike.modelled;
use schema citibike.raw;


create or replace stage streaming_data
  url = 's3://citibike-snowpipe/'
  storage_integration = citibike_snowpipe
  file_format=(type=json);

ls @streaming_data;
rm @streaming_data;


-- create the sproc that streams data out into S3 - this is the "generator"
create or replace procedure stream_data (START_DATE STRING, STOP_DATE STRING)
returns float
language javascript strict
as
$$
var counter = 0;

// list the partition values
var days = snowflake.execute({ sqlText: `
    select 
        distinct to_char(date(starttime))
    from demo.trips_stations_weather_vw
    where 
        to_date(starttime) >= to_date('` + START_DATE + `')
        and to_date(starttime) <= to_date('` + STOP_DATE + `')
    order by 1;` });


// for each partition
while (days.next())
{
    var day = days.getColumnValue(1);
    var unload_qry = snowflake.execute({ sqlText: `
    copy into @streaming_data/` + day + ` from (
      select object_construct(*)
      from (select STARTTIME,STOPTIME,START_STATION_ID,START_STATION,
                   START_REGION,START_BOROUGH,START_NEIGHBORHOOD,START_LAT,
                   START_LON,END_STATION_ID,END_STATION,END_REGION,END_BOROUGH,
                   END_NEIGHBORHOOD,END_LAT,END_LON,BIKEID,USERTYPE,BIRTH_YEAR,
                   GENDER,TEMP_AVG_C,TEMP_AVG_F,WIND_DIR,WIND_SPEED_MPH,WIND_SPEED_KPH
        from demo.trips_stations_weather_vw
        where to_date(starttime) = to_date('` + day + `')
    order by starttime));` });

    counter++;

    // sleep for five seconds
    var wake = new Date();
    var now = new Date();
    wake = Date.now() + 5000;
    do { now = Date.now(); }
      while (now <= wake);
}

return counter;
$$;


create or replace procedure purge_files (TABLE_NAME STRING, STAGE_NAME STRING)
  returns float
  language javascript strict
  execute as caller
as
$$
  var counter = 0;
  var sqlRemove = "";
  var sqlFiles = "select listagg('.*' || h.file_name, '|'), count(distinct h.file_name)" +
                 "  from table(information_schema.copy_history(" +
                 "    table_name=>'" + TABLE_NAME + "'," +
                 "    start_time=>dateadd(hour, -10, current_timestamp))) h" +
                 "  inner join (select distinct metadata$filename filename from " + STAGE_NAME  + ") f" +
                 "    on f.filename = (h.stage_location || h.file_name)" +
                 "  where h.error_count = 0;"
  // list the files to purge
  var files = snowflake.execute({ sqlText: sqlFiles });
  // for each file
  while (files.next())
  {
    var file = files.getColumnValue(1);
    sqlRemove = "rm " + STAGE_NAME + " pattern='" + file + "';";
    try {
        var unload_qry = snowflake.execute({ sqlText: sqlRemove });
        counter = files.getColumnValue(2);
    }
    catch (err) {
        counter = 0;
    }
}
  return counter;
$$;


/*********************************************************************
  Create the landing table and the streams to track new inserts
*********************************************************************/

-- create target table for initial ingestion
create or replace table trips_raw (v variant);


-- create the pipe that loads data from the staging area into the loading table
create or replace pipe trips_pipe auto_ingest=true as copy into trips_raw from @streaming_data/;
show pipes;


-- create the streams to track ingestion
create or replace stream new_trips on table citibike.raw.trips_raw;
create or replace stream new_stations on table citibike.raw.trips_raw;
create or replace stream new_neighborhoods on table citibike.raw.trips_raw;
show streams;



/*********************************************************************
  Let's run a test of the ingestion
*********************************************************************/

-- unload a single day of data
call stream_data('2019-01-01', '2019-01-01');

-- see the resulting files in S3
ls @streaming_data;

-- run this to see Snowpipe detect the files and process them
select system$pipe_status('trips_pipe');

-- the records will be deposited into this table
select * from trips_raw;

-- Snowpipe keeps track of the processed files here
select *
from table(information_schema.copy_history(
  table_name=>'citibike.raw.trips_raw',
  start_time=>dateadd(hour, -1, current_timestamp)));

-- the streams will keep track of changes (inserts) to the landing table
select count(*) from new_trips;

select * from new_trips limit 100;



/*********************************************************************
  Create the modelled schema
*********************************************************************/

create or replace table modelled.trips (
  starttime timestamp_ntz,
  stoptime timestamp_ntz,
  tripduration integer,
  start_station_id integer,
  end_station_id integer,
  bikeid integer,
  usertype string,
  birth_year integer,
  gender integer,
  last_updated timestamp_ntz);

create or replace table modelled.stations (
  station_id integer,
  station_name string,
  station_lat float,
  station_lon float,
  station_neighborhood string,
  last_updated timestamp_ntz
);

create or replace table modelled.neighborhoods (
  neighborhood_name string,
  borough_name string,
  region_name string,
  last_updated timestamp_ntz
);



/*********************************************************************
  Create the tasks to shift records from the landing table to the
  modelled schema tables
*********************************************************************/

create warehouse if not exists hol_wh with warehouse_size = 'small' auto_suspend = 300 initially_suspended = true;


create or replace task push_trips 
  warehouse = hol_wh
  schedule = '1 minute'
  when system$stream_has_data('new_trips')
  as
  insert into modelled.trips
    select
    v:STARTTIME::timestamp_ntz,
    v:STOPTIME::timestamp_ntz,
    datediff('minute', v:STARTTIME::timestamp_ntz, v:STOPTIME::timestamp_ntz),
    v:START_STATION_ID::integer,
    v:END_STATION_ID::integer,
    v:BIKEID::integer,
    v:USERTYPE::string,
    v:BIRTH_YEAR::integer,
    v:GENDER::integer,
    current_timestamp::timestamp_ntz
    from new_trips;


create or replace task push_stations 
  warehouse = hol_wh
  schedule = '1 minute'
  when system$stream_has_data('new_stations')
  as
  merge into modelled.stations s
    using (
      select 
        v:START_STATION_ID::integer station_id,
        v:START_STATION::string station_name,
        v:START_LAT::float station_lat,
        v:START_LON::float station_lon,
        v:START_NEIGHBORHOOD::string station_neighborhood,
        current_timestamp::timestamp_ntz last_updated
      from new_stations
      union
      select 
        v:END_STATION_ID::integer station_id,
        v:END_STATION::string station_name,
        v:END_LAT::float station_lat,
        v:END_LON::float station_lon,
        v:END_NEIGHBORHOOD::string station_neighborhood,
        current_timestamp::timestamp_ntz last_updated
      from new_stations) ns
      on s.station_id = ns.station_id
    when not matched then
      insert (station_id, station_name, station_lat, station_lon, station_neighborhood, last_updated)
      values (ns.station_id, ns.station_name, ns.station_lat, ns.station_lon, ns.station_neighborhood, ns.last_updated);


create or replace task push_neighborhoods
  warehouse = hol_wh
  schedule = '1 minute'
  when system$stream_has_data('new_neighborhoods')
  as
  merge into modelled.neighborhoods n
    using (
      select 
        v:START_NEIGHBORHOOD::string neighborhood_name,
        v:START_BOROUGH::string borough_name,
        v:START_REGION::string region_name,
        current_timestamp::timestamp_ntz last_updated
      from new_neighborhoods
      union
      select 
        v:END_NEIGHBORHOOD::string neighborhood_name,
        v:END_BOROUGH::string borough_name,
        v:END_REGION::string region_name,
        current_timestamp::timestamp_ntz last_updated
      from new_neighborhoods) nn
      on n.neighborhood_name = nn.neighborhood_name
    when not matched then
      insert (neighborhood_name, borough_name, region_name, last_updated)
      values (nn.neighborhood_name, nn.borough_name, nn.region_name, nn.last_updated);
    

create or replace task purge_files
  warehouse = hol_wh
  schedule = '1 minute'
  as
    call purge_files('trips_raw', '@streaming_data');



/*********************************************************************
  Enable the tasks so they will run and process the data
  in the streams
*********************************************************************/

alter task purge_files resume;
alter task push_trips resume;
alter task push_stations resume;
alter task push_neighborhoods resume;



/*********************************************************************
  Now monitor the data pipeline...
*********************************************************************/

select 
  (select count(distinct metadata$filename) from @streaming_data) num_files_in_bucket,
  (select count(*) from table(information_schema.copy_history(table_name=>'trips_raw', start_time=>dateadd(hour, -1, current_timestamp)))) num_files_processed,
  (select count(*) from trips_raw) recs_in_landing_table,
  (select count(*) from new_trips) recs_in_stream,
  (select count(*) from modelled.trips) recs_in_modelled_table;

-- here are the modelled tables we have built
select * from modelled.trips order by last_updated desc limit 100;
select * from modelled.stations;
select * from modelled.neighborhoods;



/*********************************************************************
  Here's the code for the generator script
  Run this in a separate tab to simulate a streaming source
*********************************************************************/

-- set the context
use role dba_citibike;
create warehouse if not exists demo_wh with warehouse_size = 'small' auto_suspend = 300 initially_suspended = true;
use warehouse demo_wh;
use schema citibike.raw;

call stream_data('2019-01-02', '2020-01-01');
