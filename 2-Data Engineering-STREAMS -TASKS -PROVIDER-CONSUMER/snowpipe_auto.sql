/*--------------------------------------------------------------------------------
  SNOWPIPE AUTO V2

  Run this in your demo account.

  This script shows Snowpipe handling continuous ingestion from a S3 bucket.

  Author:   Alan Eldridge
  Updated:  18 Nov 2019 (aeldridge) - updated for V2.7 schema changes
            12 Dec 2019 (mprotz) - fixed some errors

  #snowpipe #streaming
--------------------------------------------------------------------------------*/

use role dba_citibike;
create warehouse if not exists load_wh with warehouse_size = 'medium' auto_suspend = 300 initially_suspended = true;
alter warehouse if  exists load_wh set warehouse_size = 'medium';
use warehouse load_wh;
use schema citibike.demo;

/*--------------------------------------------------------------------------------
  This time we are streaming trip data into the database
--------------------------------------------------------------------------------*/

-- create a stage where the files will land

-- ***
-- *** If this is the first time you run this script, you need to put your own
-- *** demo directory in the URL below. The URL format is:
-- ***   - s3://<bucket>/<path>
-- *** This is necessary because a bucket cannot bind to more than one account
-- *** at a time. This also ensures we don't collide with each other when
-- *** we run the demo.
-- ***
-- *** You also need to configure your bucket to send event notifications to
-- *** your Snowflake account. Instructions are here:
-- *** https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe-auto-s3.html
-- ***

create or replace stage utils.pipe_data
  url = 'your s3 bucket url goes here'
  credentials = (aws_key_id = 'your key goes here' aws_secret_key = 'your secret key goes here')
  file_format=utils.csv_no_header;

show stages like '%PIPE%' in schema utils;

-- Note the notification channel - this is how S3 advises us about new arrivals

-- create the table into which the data will be loaded
create or replace table trips_stream like trips;

-- create the pipe
create or replace pipe trip_pipe auto_ingest=true as
  copy into trips_stream from @utils.pipe_data/;

show pipes;

-- Note the same notification channel - the pipe is listening for events


/*--------------------------------------------------------------------------------
  Load some files into the stage - either via AWS Console or S3 loading tool

  Use the files in the snowpipe_data directory.
--------------------------------------------------------------------------------*/

-- show the files in the stage
list @utils.pipe_data;

select system$pipe_status('trip_pipe');

-- show the files that have been processed
select *
from table(information_schema.copy_history(table_name=>'TRIPS_STREAM', start_time=>dateadd('hour', -1, CURRENT_TIMESTAMP())));

-- show the data landing in the table
select count(*) from trips_stream;
