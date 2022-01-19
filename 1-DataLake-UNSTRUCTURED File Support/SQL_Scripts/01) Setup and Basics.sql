--------- Part 0: Setup  ---------
use role sysadmin;
create database if not exists demo_db;
use schema demo_db.public;
create or replace warehouse demo_wh warehouse_size = 'x-small' auto_suspend=300;
use warehouse demo_wh;

-- Create internal stage that has type SNOWFLAKE_SSE (Service Side Encryption)
create or replace stage images_stage encryption = (type = 'SNOWFLAKE_SSE');

/* --- 
-- Using SnowSQL put some files into this stage 
-- This is an example for my account, you will need to use your account
snowsql -a XXXXXXXXXX.us-east-1 -u john -r sysadmin -d demo_db -s public -w demo_wh

-- Here is an example that will PUT all the files in the folder to the internal stage
put file:///Users/crichardson/Documents/git/Chris-Richardson_REPO/SnowflakeFeatureDemo-FileSupport/Data_Files/10files/* @images_stage/images/ AUTO_COMPRESS=false;
-- */

-- List the files  
ls @images_stage; 

-- Look at the stage metadata
select metadata$filename, METADATA$ABSOLUTE_PATH, METADATA$RELATIVE_PATH, GET_PRESIGNED_URL(@images_stage, METADATA$RELATIVE_PATH) from @images_stage;

--------- Part 1 : Basics  ---------
-- Lets show the new function get_presigned_url
select 
  distinct metadata$filename file_name
  , get_presigned_url(@images_stage, file_name) signed_url
from @images_stage;

-- If you copy and paste the signed URL you can download the file, we are also going to wrap in image tags for use in Zepl (or other tools)
select 
  distinct metadata$filename file_name,
  concat('<img width="125” height=“80" src="', get_presigned_url(@images_stage, file_name) ,'"></img>') image
  , concat('<a href="', get_presigned_url(@images_stage, file_name) ,'"> download </a>') link
  , get_presigned_url(@images_stage, file_name) signed_url
from @images_stage;


-- Show the images Zepl 
-- https://app.zepl.com/OR9QI394D/notebooks/e498acbca31b447fad8748bfb30f33e7



