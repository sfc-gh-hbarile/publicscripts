/*--------------------------------------------------------------------------------
  SCD TYPE 2 DATAS PIPELINE V2

  Run this in your demo account.

  This script creates a SCD Type 2 framework on the STATIONS table. It uses
  streams and tasks to track changes to the table and push them into the
  STATIONS_HISTORY table.

  Based on an original concept from John Gontarz. Thanks for contributing!

  Author:   Alan Eldridge
  Updated:  18 Nov 2019 (aeldridge) - updated for V2.7 schema changes

  #optional #streams #tasks #scd #pipeline
--------------------------------------------------------------------------------*/

use role dba_citibike;
create warehouse if not exists load_wh with warehouse_size = 'medium' auto_suspend = 300;
use warehouse load_wh;
use schema citibike.demo;

/*--------------------------------------------------------------------------------
  Let's review our STATIONS table
--------------------------------------------------------------------------------*/

-- get a clean copy of the STATIONS table
create or replace table stations as
  select station_id, station_name, station_latitude, station_longitude, station_comment
  from snowflake_demo_resources.citibike_reset_v2.stations;

-- this is what it looks like
select * from stations order by station_id;

-- let's empty the table for this demo
truncate table stations;


/*--------------------------------------------------------------------------------
  We want to capture changes to our STATIONS table - inserts, updates, deletes - so
  let's keep a STATIONS_HISTORY table with copies of all the changed records.

  This is called a Slowly Changing Dimension Type II table.
--------------------------------------------------------------------------------*/

-- create the history table where we will keep the changes to the STATIONS records
create or replace table stations_history like stations;
alter table stations_history add column start_time timestamp_ntz, end_time timestamp_ntz, current_flag integer;

desc table stations_history;

-- add a column to our stations table to track when the record was last updated
alter table stations add column update_timestamp timestamp_ntz;

desc table stations;


/*--------------------------------------------------------------------------------
  We create a stream object to capture the changes to the STATIONS table
--------------------------------------------------------------------------------*/

create or replace stream stations_table_changes on table stations;

show streams;

-- no data has been loaded or changed so the stream will be empty
select * from stations_table_changes;


/*--------------------------------------------------------------------------------
  This is the logic that handles the merge into the STATIONS table and the
  STATIONS_HISTORY table.

  It's very complex. Trust me, it works.
--------------------------------------------------------------------------------*/

-- View to help generate the data to be loaded into the STATIONS_HISTORY table
create or replace view change_data as
-- This sub-query figures out what to do when data is inserted into the STATIONS table
-- An insert to the STATIONS table results in an INSERT to the STATIONS_HISTORY table
select station_id, station_name, station_latitude, station_longitude, station_comment, start_time, end_time, current_flag, 'I' as dml_type
from (select station_id, station_name, station_latitude, station_longitude, station_comment,
             update_timestamp as start_time,
             lag(update_timestamp) over (partition by station_id order by update_timestamp desc) as end_time_raw,
             case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
             case when end_time_raw is null then 1 else 0 end as current_flag
      from (select station_id, station_name, station_latitude, station_longitude, station_comment, update_timestamp
            from stations_table_changes
            where metadata$action = 'INSERT'
            and   metadata$isupdate = 'FALSE'))
union
-- This sub-query figures out what to do when data is updated in the STATIONS table
-- An update to the STATIONS table results in an update AND an insert to the STATIONS_HISTORY table
-- The sub-query below generates two records each with a different dml_type
select station_id, station_name, station_latitude, station_longitude, station_comment, start_time, end_time, current_flag, dml_type
from (select station_id, station_name, station_latitude, station_longitude, station_comment,
             update_timestamp as start_time,
             lag(update_timestamp) over (partition by station_id order by update_timestamp desc) as end_time_raw,
             case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
             case when end_time_raw is null then 1 else 0 end as current_flag,
             dml_type
      from (-- Identify data to insert into nation_history table
            select station_id, station_name, station_latitude, station_longitude, station_comment, update_timestamp, 'I' as dml_type
            from stations_table_changes
            where metadata$action = 'INSERT'
            and   metadata$isupdate = 'TRUE'
            union
            -- Identify data in nation_history table which needs to be udpated
            select station_id, null, null, null, null, start_time, 'U' as dml_type
            from stations_history
            where station_id in (select distinct station_id
                                  from stations_table_changes
                                  where metadata$action = 'INSERT'
                                  and   metadata$isupdate = 'TRUE')
            and   current_flag = 1))
union
-- This sub-query figures out what to do when data is deleted from the STATIONS table
-- A deletion from the STATIONS table results in an update to the STATIONS_HISTORY table
select stc.station_id, null, null, null, null, sh.start_time, current_timestamp()::timestamp_ntz, null, 'D'
from stations_history sh
inner join stations_table_changes stc
   on sh.station_id = stc.station_id
where stc.metadata$action = 'DELETE'
and   stc.metadata$isupdate = 'FALSE'
and   sh.current_flag = 1;


/*--------------------------------------------------------------------------------
  OK - now let's insert some data into STATIONS
--------------------------------------------------------------------------------*/

-- get the current time
set update_timestamp = current_timestamp()::timestamp_ntz;

-- insert 4 records into the STATIONS table (which is currently empty)
insert into stations
  select station_id, station_name, station_latitude, station_longitude, station_comment, $update_timestamp
    from snowflake_demo_resources.citibike_reset_v2.stations
    where station_id < 100;

-- look at the the STATIONS table
select * from stations order by station_id;

-- look at the stream - it contains records for all the inserts
select * from stations_table_changes;

-- look at the STATIONS_HISTORY table - current it is empty as we haven't run the MERGE statement
select * from stations_history order by station_id;


/*--------------------------------------------------------------------------------
  Push the updates to the STATION_HISTORY table
--------------------------------------------------------------------------------*/

-- MERGE statement that uses the CHANGE_DATA view to load data into the STATIONS_HISTORY table
merge into stations_history sh -- Target table to merge changes from STATION into
using change_data m -- change_data is a view that holds the logic determines what to insert / update into the station_history table.
   on  sh.station_id = m.station_id -- station_id and start_time are what determines a unique record in the station_history table
   and sh.start_time = m.start_time
when matched and m.dml_type = 'U' then update -- When there is an Update to the record is no longer current and the end_time needs to be stamped
    set sh.end_time = m.end_time,
        sh.current_flag = 0
when matched and m.dml_type = 'D' then update -- Deletes are essentially logical deletes.  The record is stamped and no newer version is inserted
    set sh.end_time = m.end_time,
        sh.current_flag = 0
when not matched and m.dml_type = 'I' then insert -- Inserting a new station_id and updating an existing one both result in an Insert
           (station_id, station_name, station_latitude, station_longitude, station_comment, start_time, end_time, current_flag)
    values (m.station_id, m.station_name, m.station_latitude, m.station_longitude, m.station_comment, m.start_time, m.end_time, m.current_flag);


-- look at the STATIONS_HISTORY table
select * from stations_history order by station_id;

-- look at the stream - it is now empty as the MERGE emptied it
select * from stations_table_changes;


/*--------------------------------------------------------------------------------
  So the process works! But rather than us running the MERGE statement manually
  every time we make a change, we want this to happend automatically.

  To do this, we create a TASK, and have it run the MERGE statement regularly.
--------------------------------------------------------------------------------*/

-- create a dedicated warehouse to run the task
create warehouse if not exists task_wh with warehouse_size = 'xsmall' auto_suspend = 60;
use warehouse load_wh;

-- create the task to schedule the MERGE statement once every minute
create or replace task populate_station_history warehouse = task_wh schedule = '1 minute' when system$stream_has_data('stations_table_changes')
as
merge into stations_history sh
using change_data m
   on  sh.station_id = m.station_id
   and sh.start_time = m.start_time
when matched and m.dml_type = 'U' then update
    set sh.end_time = m.end_time,
        sh.current_flag = 0
when matched and m.dml_type = 'D' then update
    set sh.end_time = m.end_time,
        sh.current_flag = 0
when not matched and m.dml_type = 'I' then insert
           (station_id, station_name, station_latitude, station_longitude, station_comment, start_time, end_time, current_flag)
    values (m.station_id, m.station_name, m.station_latitude, m.station_longitude, m.station_comment, m.start_time, m.end_time, m.current_flag);

-- Resume task to make it run
alter task populate_station_history resume;
show tasks;

-- When will the next task run?
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;

select * from table(information_schema.task_history());


/*--------------------------------------------------------------------------------
  Let's update some records in the STATION_HISTORY table
--------------------------------------------------------------------------------*/

-- update two records
update stations
  set station_comment = '*** New comment for station 72', update_timestamp = current_timestamp()::timestamp_ntz
  where station_id = 72;

update stations
  set station_comment = '*** New comment for station 79', update_timestamp = current_timestamp()::timestamp_ntz
  where station_id = 79;

-- look at the stations table
select * from stations order by 1;

-- look at the stream
select * from stations_table_changes;

-- How long do we have to wait?
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;

-- look at the history table
select * from stations_history order by station_id, start_time;


/*--------------------------------------------------------------------------------
  Let's delete some records
--------------------------------------------------------------------------------*/

delete from stations where station_id = 72;
delete from stations where station_id = 79;

-- look at the stations table
select * from stations order by 1;

-- look at the stream
select * from stations_table_changes;

-- How long do we have to wait?
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;

-- look at the history table
select * from stations_history order by station_id, start_time;


/*--------------------------------------------------------------------------------
  Now any changes we make will be accumulated in the stream and then merged into
  the history table every minute.

  Note that this is not an audit stream - events that overwrite each other will
  only show as the final delta in the stream...
--------------------------------------------------------------------------------*/

insert into stations
  select station_id, station_name, station_latitude, station_longitude, station_comment, $update_timestamp
    from snowflake_demo_resources.citibike_reset_v2.stations
    where station_id < 80;

update stations
  set station_comment = '*** Totally new comment for station 72', update_timestamp = current_timestamp()::timestamp_ntz
  where station_id = 72;

update stations
  set station_comment = '*** Totally new for station 79', update_timestamp = current_timestamp()::timestamp_ntz
  where station_id = 79;

delete from stations where station_id = 79;


-- look at the stations table
select * from stations order by 1;

-- look at the stream
select * from stations_table_changes;

-- How long do we have to wait?
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;

-- look at the history table
select * from stations_history order by station_id, start_time;
