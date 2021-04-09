-- While still in private peview you will need to enable these parameters in the Snowflake account
-- For example => https://snowflake.prod2.external-zone.snowflakecomputing.com/


alter account va_demo49 set 
    enable_get_presigned_url = true,
    enable_get_absolute_path = true,
    enable_get_stage_location = true,
    ENABLE_UNENCRYPTED_INTERNAL_STAGES=true
parameter_comment = 'SE Demo Account testing' 
parameter_expiry_days = 90;  