/*--------------------------------------------------------------------------------
  DATA PIPELINE DEMO - GENERATOR

  #3 in the pipeline demo.
  Run this in your demo account.

  Creates the schemas, stored procedures and such.

  Author:   Alan Eldridge
  Updated:  26 July 2019
  18 Nov 2019 (aeldridge) - updated for V2.7 schema changes

  #streaming #storedprocs #externalstage
--------------------------------------------------------------------------------*/

-- set the context
use role dba_citibike;
create warehouse if not exists demo_wh with warehouse_size = 'small' auto_suspend = 300 initially_suspended = true;
use warehouse demo_wh;
use schema citibike.raw;

call stream_data('2019-01-02', '2020-01-01');
