--------- Part 2 : External Function with AWS Rekognition ---------

-- AWS account ID: 999999999999
-- API Gateway: https://rbsmq5ez3l.execute-api.us-east-1.amazonaws.com/
-- Role for Demo: arn:aws:iam::999999999999:role/sf-aws-api-role
-- Role that execute the Lambda functions: arn:aws:iam::999999999999:role/sf-lambda-role

-- create API integration

use role accountadmin;

create or replace api integration se_sandbox_api_integration
   api_provider=aws_api_gateway
   api_aws_role_arn='arn:aws:iam::999999999999:role/sf-aws-api-role'
   api_allowed_prefixes=('https://rbsmq5ez3l.execute-api.us-east-1.amazonaws.com/')
   enabled=true;

grant usage on integration se_sandbox_api_integration to sysadmin;

desc integration se_sandbox_api_integration;

-- In the AWS account you will need to add your API_AWS_IAM_USER_ARN and API_AWS_EXTERNAL_ID and to the trust relationship on the role we used above: arn:aws:iam::999999999999:role/sf-aws-api-role
-- AWS > IAM > Roles > search for sf-aws-api-role > Trust Relationships tab > Edit (see below) > Save (update)
-- For Edit copy and paste a block and replace with your information:
-- "Principal": {"AWS": "API_AWS_IAM_USER_ARN" }
-- "sts:ExternalId": "API_AWS_EXTERNAL_ID"

use role sysadmin;
use schema demo_db.public;
use warehouse demo_wh;

-- What images do we have staged
ls @images_stage/;

-- Get a presigned url
select 
  distinct metadata$filename file_name
  , get_presigned_url(@images_stage, file_name) signed_url
from @images_stage

-- Create a function 
create or replace external function sf_label_image(presigned_url string)
returns variant
api_integration = se_sandbox_api_integration
as 'https://rbsmq5ez3l.execute-api.us-east-1.amazonaws.com/sf-stage/sf-label-image';

-- Use the function 
with 
get_files as (
select distinct(metadata$filename) file_name from @images_stage)
select 
  file_name
  , get_presigned_url(@images_stage, file_name) signed_url
  , sf_label_image(get_presigned_url(@images_stage, file_name)) payload from get_files;
  

---- Lets test out the external funtion using the files on the stage
with 
get_files as (
select 
   distinct(metadata$filename) file_name from @images_stage),
external_function as (
select 
  file_name
  , get_presigned_url(@images_stage, file_name) signed_url
  , sf_label_image(get_presigned_url(@images_stage, file_name)) payload from get_files)
select 
  ef.file_name
  , concat('<img width="125” height=“80" src="', get_presigned_url(@images_stage, ef.file_name) ,'"></img>') image
  , listagg(lf.value:Name::string,' | ') aws_rekognition_labels 
  , ef.payload aws_rekognition_payload
  , ef.signed_url
from 
  external_function ef
  , lateral flatten(input=>payload:response.labels, outer => TRUE ) lf
group by 1,2,4,5;

-- Now you can see the image and metadata using a single query