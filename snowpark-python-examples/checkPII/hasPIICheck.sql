/*

Created By: Parag Jain
Usage: 
Simple Python UDF for PII detection

Created On: 11/11/2021
*/

use schema tweets_db.public;
use warehouse lab_s_wh;


create table tweets (text varchar);

CREATE FILE FORMAT "TWEETS_DB"."PUBLIC".quick_load_csv TYPE = 'CSV' 
COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' 
SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' 
TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' 
TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');


create or replace function StringLength(col string)
returns number
language python
runtime_version = 3.8
handler='findLen'
as
$$
def findLen(col):
    return col.__len__()
$$;


create or replace function hasPII(col string)
returns boolean
language python
runtime_version = 3.8
handler='hasPIIText'
as
$$


def hasPIIText(col):
    phone_number = "(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})"
    ssn_number = "(?!(\d){3}(-| |)\1{2}\2\1{4})(?!666|000|9\d{2})(\b\d{3}(-| |)(?!00)\d{2}\4(?!0{4})\d{4}\b)"
    email = "(^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$)"

    regexList = [phone_number, ssn_number, email]
    gotMatch = False

    import re
    for regex in regexList:
        reg = re.compile(regex)
        check = re.search(reg,col)
        if check:
            gotMatch = True
            break
    
    if gotMatch:
        return True
    else:
        return False

$$;



select * from tweets;

select text, StringLength(text) length, hasPII(text) containPII from tweets where containPII = True;
