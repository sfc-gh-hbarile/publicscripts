------- Part 3: Streaming Image File Demo -------------------  

use role sysadmin;
create database if not exists demo_db;
use schema demo_db.public;
create or replace warehouse demo_wh warehouse_size = 'x-small' auto_suspend=300;
use warehouse demo_wh;

-- Create internal stage that has type SNOWFLAKE_SSE (Service Side Encryption)
create or replace stage images_stage encryption = (type = 'SNOWFLAKE_SSE');

-- Here is where we will stage the data
create or replace table sf_image_files (
metadata_filename string
, load_time_sf timestamp default current_timestamp
);

-- If you need to reset us this 
truncate table sf_image_files;

-- Lets create a stream on this
create or replace stream sf_image_files_stream on table sf_image_files;

-- Create a pipe
create or replace pipe sf_image_files_pipe auto_ingest=false as 
copy into sf_image_files 
from (
select distinct
metadata$filename metadata_filename
, current_timestamp()
from @images_stage/images/)
; 

describe stage images_stage;

ls @images_stage/images/

-- Note if you have an external stage you can use auto_ingest=true 
-- Set up the notification on the bucket using the notification_channel 
show pipes;

/* --- 
-- Using SnowSQL put some files into this stage
snowsql -a va_demo49.us-east-1 -u john -r sysadmin -d demo_db -s public -w demo_wh

put file:///Users/crichardson/Documents/git/Chris-Richardson_REPO/SnowflakeFeatureDemo-FileSupport/Data_Files/natural_images/animals/* @images_stage/images/ AUTO_COMPRESS=false;
put file:///Users/crichardson/Documents/git/Chris-Richardson_REPO/SnowflakeFeatureDemo-FileSupport/Data_Files/natural_images/vehicles/* @images_stage/images/ AUTO_COMPRESS=false;
*/ 

ls @images_stage/images/

-- Refresh the pipe
alter pipe sf_image_files_pipe refresh;

-- View the pipe - It may take ~1-2 min for the pipe to start
select SYSTEM$PIPE_STATUS( 'sf_image_files_pipe');  

-- The data should get loaded into the table and the stream updated
select * from sf_image_files;
select * from sf_image_files_stream;

-- Create a table for the finished files
create or replace table sf_image_files_finished (
 file_name string
, load_time_sf timestamp
, image string
, aws_rekognition_metadata string
, aws_rekognition_payload variant
, inital_signed_url string 
, finished_time_sf timestamp 
);

-- Create a task
create or replace task sf_populate_image_files_finished
  warehouse = demo_wh schedule = '1 minute' 
  when system$stream_has_data('sf_image_files_stream')
as
insert into sf_image_files_finished
with 
get_files as (
select 
  metadata_filename file_name 
  , load_time_sf
  from sf_image_files_stream),
external_function as (
select 
  file_name
  , load_time_sf
  , get_presigned_url(@images_stage, file_name) signed_url
  , concat('<img width="125” height=“80" src="', get_presigned_url(@images_stage, file_name) ,'"></img>') image
  , sf_label_image(get_presigned_url(@images_stage, file_name)) payload 
  from get_files)
select 
  ef.file_name
  , ef.load_time_sf
  , ef.image
  , listagg(lf.value:Name::string,' | ') aws_rekognition_metadata 
  , ef.payload aws_rekognition_payload
  , ef.signed_url inital_signed_url
  , current_timestamp() finished_time_sf
from 
  external_function ef
  , lateral flatten(input=>payload:response.labels, outer => TRUE ) lf
group by 1,2,3,5,6;

alter task sf_populate_image_files_finished resume;
alter task sf_populate_image_files_finished suspend;
show tasks;

-- when will the next task run? -- needs to be upper
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history()) where name='SF_POPULATE_IMAGE_FILES_FINISHED' order by completed_time desc;

select *
from table(information_schema.task_history()) where name='SF_POPULATE_IMAGE_FILES_FINISHED';


select * from sf_image_files_finished;

-- This view will always have a "live" signed url
create or replace view sf_image_files_finished_vw as 
select 
  file_name
  , load_time_sf loaded
  , concat('<img width="125” height=“80" src="', get_presigned_url(@images_stage, file_name) ,'"></img>') image
  , concat('<a href="', get_presigned_url(@images_stage, file_name) ,'"> download </a>') link
  , aws_rekognition_metadata
from 
  sf_image_files_finished;
  
select * from sf_image_files_finished_vw;