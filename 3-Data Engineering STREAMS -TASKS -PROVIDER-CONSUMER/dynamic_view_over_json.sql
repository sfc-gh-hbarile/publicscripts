/*--------------------------------------------------------------------------------
  DYNAMIC VIEW OVER JSON

  Run this in your demo account.

  This script automatically creates a view over a JSON variant column, surfacing
  all the attributes as columns.

  Author:   Craig Warman
  Updated:  12 December 2019

  #optional #semi-structured #json #view
--------------------------------------------------------------------------------*/

-- set the context
use role dba_citibike;
create warehouse if not exists load_wh with warehouse_size = 'small' auto_suspend = 300 initially_suspended = true;
alter warehouse if  exists load_wh set warehouse_size = 'small';
use warehouse load_wh;
use schema citibike.utils;

show procedures;

create or replace procedure create_view_over_json (TABLE_NAME varchar, COL_NAME varchar, VIEW_NAME varchar, COLUMN_CASE varchar, COLUMN_TYPE varchar)
returns varchar
language javascript
as
$$
// CREATE_VIEW_OVER_JSON - Craig Warman, Snowflake Computing, DEC 2019
//
// This stored procedure creates a view on a table that contains JSON data in a column.
// of type VARIANT.  It can be used for easily generating views that enable access to
// this data for BI tools without the need for manual view creation based on the underlying
// JSON document structure.
//
// Parameters:
// TABLE_NAME    - Name of table that contains the semi-structured data.
// COL_NAME      - Name of VARIANT column in the aforementioned table.
// VIEW_NAME     - Name of view to be created by this stored procedure.
// COLUMN_CASE   - Defines whether or not view column name case will match
//                 that of the corresponding JSON document attributes.  When
//                 set to 'uppercase cols' the view column name for a JSON
//                 document attribute called "City.Coord.Lon" would be generated
//                 as "CITY_COORD_LON", but if this parameter is set to
//                 'match col case' then it would be generated as "City_Coord_Lon".
// COLUMN_TYPE   - The datatypes of columns generated for the view will match
//                 those of the corresponding JSON data attributes if this param-
//                 eter is set to 'match datatypes'. But when this parameter is
//                 set to 'string datatypes' then the datatype of all columns
//                 in the resulting view will be set to STRING (VARCHAR).
//
// Usage Example:
// call create_view_over_json('db.schema.semistruct_data', 'variant_col', 'db.schema.semistruct_data_vw', 'match col case', 'match datatypes');
//
// Important notes:
//   - JSON documents may contain attributes that are actually reserved words, which
//     may cause SQL complilation errors to be thrown during the CREATE VIEW execution.
//     The easiest work-around in this case is to specify the 'match col case' for the
//     COLUMN_CASE parameter since this causes column names to be enclosed by double
//     quotes when the view is generated.
//   - Sometimes there cases where the JSON documents attributes with the same name
//     but actually contain data with different datatypes.  For example, one document
//     might have an attribute that contains the value "10" while another document
//     has the same attribute with a value of "ten".  This may lead to problems since
//     Snowflake will interpret the datatype of the first document as being numeric,
//     while the second would be a string value.  Specifying 'string datatypes' for the
//     COLUMN_TYPE parameter should help alleviate such issues.
//   - Column names for arrays in the JSON document structure will be prefixed by the
//     path to the array.  For example, consider a simple array such as:
//                  "code": {
//                    "rgb": [255,255,0]
//     The corresponding view columns in this case would be code_rgb_0, code_rgb_1, and
//     code_rgb_2.
//   - Column names for object arrays are similarly generated.  For example, consider an
//     object array such as:
//                 contact: {
//                     phone: [
//                       { type: "work", number:"404-555-1234" },
//                       { type: "mobile", number:"770-555-1234" }
//     The corresponding view columns in this case would be contact_phone_type and
//     contact_phone_number.
//   - This procedure will work for arrays that are one level deep in the JSON structure.
//     Arrays within arrays will be materialized in the view as columns of type ARRAY.
//   - Execution of this procedure may take an extended period of time for very
//     large datasets, or for datasets with a wide variety of document attributes
//     (since the view will have a large number of columns)
//
// Attribution:
// I leveraged code developed by Alan Eldridge as the basis for this stored procedure.

var alias_dbl_quote = "";
var path_name = "regexp_replace(regexp_replace(f.path,'\\\\[(.+)\\\\]'),'(\\\\w+)','\"\\\\1\"')"   // This generates paths with levels enclosed by double quotes (ex: "path"."to"."element").  It also strips any bracket-enclosed array element references (like "[0]")
var attribute_type = "DECODE (substr(typeof(f.value),1,1),'A','ARRAY','B','BOOLEAN','I','FLOAT','D','FLOAT','STRING')";    // This generates column datatypes of ARRAY, BOOLEAN, FLOAT, and STRING only
var alias_name = "REGEXP_REPLACE(REGEXP_REPLACE(f.path, '\\\\[(.+)\\\\]'),'[^a-zA-Z0-9]','_')" ;                           // This generates column aliases based on the path
var table_list = TABLE_NAME;
var col_list = "";
var array_num = 0;

if (COLUMN_CASE.toUpperCase().charAt(0) == 'M') {
   alias_dbl_quote = "\""; }          // COLUMN_CASE parameter is set to 'match col case' so add double quotes around view column alias name
if (COLUMN_TYPE.toUpperCase().charAt(0) == 'S') {
   attribute_type = "DECODE (typeof(f.value),'ARRAY','ARRAY','STRING')"; }   // COLUMN_TYPE parameter is set to 'string datatypes' so typecast to STRING instead of value returned by TYPEPOF function

// Build a query that returns a list of elements which will be used to build the column list for the CREATE VIEW statement
var element_query = "SELECT DISTINCT \n" +
                    path_name + " AS path_name, \n" +
                    attribute_type + " AS attribute_type, \n" +
                    alias_name + " AS alias_name \n" +
                    "FROM \n" +
                    TABLE_NAME + ", \n" +
                    "LATERAL FLATTEN(" + COL_NAME + ", RECURSIVE=>true) f \n" +
                    "WHERE TYPEOF(f.value) != 'OBJECT' \n" +
                    "AND NOT contains(f.path,'[') ";      // This prevents traversal down into arrays

// Run the query...
var element_stmt = snowflake.createStatement({sqlText:element_query});
var element_res = element_stmt.execute();

// ...And loop through the list that was returned
while (element_res.next()) {

// Add any non-array elements and datatypes to the column list
// They will look something like this when added:
//    col_name:"name"."first"::STRING as "name_first",
//    col_name:"name"."last"::STRING as "name_last"
// Note that double-quotes around the column aliases will be added
// only when the COLUMN_CASE parameter is set to 'match col case'

   if (element_res.getColumnValue(2) != 'ARRAY') {
      if (col_list != "") {
         col_list += ", \n";}
      col_list += COL_NAME + ":" + element_res.getColumnValue(1);                               // Start with the element path name
      col_list += "::" + element_res.getColumnValue(2);                                         // Add the datatype
      col_list += " as " + alias_dbl_quote + element_res.getColumnValue(3) + alias_dbl_quote;   // And finally the element alias
   }

// Array elements get handled in the following section:
   else {
      array_num++;
      var simple_array_col_list = "";
      var object_array_col_list = "";

// Build a query that returns the elements in the current array
      var array_query = "SELECT DISTINCT \n"+
                         path_name + " AS path_name, \n" +
                         attribute_type + " AS attribute_type, \n" +
                         alias_name + " AS attribute_name, \n" +
                         "f.index \n" +
                         "FROM \n" +
                         TABLE_NAME + ", \n" +
                         "LATERAL FLATTEN(" + COL_NAME + ":" + element_res.getColumnValue(1) + ", RECURSIVE=>true) f \n" +
                         "WHERE REGEXP_REPLACE(f.path, '.+(\\\\w+\\\\[.+\\\\]).+', 'SubArrayEle') != 'SubArrayEle' ";  // This prevents return of elements of arrays within arrays (the entire array will be returned in this case)

// Run the query...
      var array_stmt = snowflake.createStatement({sqlText:array_query});
      var array_res = array_stmt.execute();

// ...And loop through the list that was returned.
// Add array elements and datatypes to the column list
// The way that they're added depends on the type of array:
//
// Simple arrays:
// These are lists of values that are addressible by their index number
//   For example:
//      "code": {
//         "rgb": [255,255,0]
// These will be added to the view column list like so:
//    col_name:"code"."rgb"[0]::FLOAT as code_rgb_0,
//    col_name:"code"."rgb"[1]::FLOAT as code_rgb_1,
//    col_name:"code"."rgb"[2]::FLOAT as code_rgb_2
//
// Object arrays:
// Collections of objects that addressible by key
// For example:
//     contact: {
//         phone: [
//           { type: "work", number:"404-555-1234" },
//           { type: "mobile", number:"770-555-1234" }
// These will be added to the view column list like so:
//    a1.value:"type"::STRING as "phone_type",
//    a1.value:"number"::STRING as "phone_number"
// Along with an additional LATERAL FLATTEN construct in the table list:
//    FROM mydatabase.public.contacts,
//     LATERAL FLATTEN(json_data:"contact"."phone") a1;
//

      while (array_res.next()) {
         if (array_res.getColumnValue(1).substring(1) == "") {              // The element path name is empty, so this is a simple array element
             if (simple_array_col_list != "") {
                simple_array_col_list += ", \n";}
             simple_array_col_list += COL_NAME + ":" + element_res.getColumnValue(1);    // Start with the element path name
             simple_array_col_list += "[" + array_res.getColumnValue(4) + "]";           // Add the array index
             simple_array_col_list += "::" + array_res.getColumnValue(2);                // Add the datatype
             simple_array_col_list += " as " + alias_dbl_quote + element_res.getColumnValue(3) + "_" + array_res.getColumnValue(4) + alias_dbl_quote;   // And finally the element alias - Note that the array alias is added as a prefix to ensure uniqueness
             }
         else {                                                             // This is an object array element
             if (object_array_col_list != "") {
                object_array_col_list += ", \n";}
             object_array_col_list += "a" + array_num + ".value:" + array_res.getColumnValue(1).substring(1);    // Start with the element name (minus the leading '.' character)
             object_array_col_list += "::" + array_res.getColumnValue(2);                                        // Add the datatype
             object_array_col_list += " as " + alias_dbl_quote + element_res.getColumnValue(3) + array_res.getColumnValue(3) + alias_dbl_quote;   // And finally the element alias - Note that the array alias is added as a prefix to ensure uniqueness
             }
      }


// If no object array elements were found then add the simple array elements to the
// column list...
      if (object_array_col_list == "") {
          if (col_list != "") {
             col_list += ", \n";}
          col_list += simple_array_col_list;
          }
// ...otherwise, add the object array elements to the column list along with a
// LATERAL FLATTEN clause that references the current array to the table list
      else {
          if (col_list != "") {
             col_list += ", \n";}
          col_list += object_array_col_list;
          table_list += ",\n LATERAL FLATTEN(" + COL_NAME + ":" + element_res.getColumnValue(1) + ") a" + array_num;
          }
   }

}

// Now build the CREATE VIEW statement
var view_ddl = "CREATE OR REPLACE VIEW " + VIEW_NAME + " AS \n" +
               "SELECT \n" + col_list + "\n" +
               "FROM " + table_list;

// Now run the CREATE VIEW statement
var view_stmt = snowflake.createStatement({sqlText:view_ddl});
var view_res = view_stmt.execute();
return view_res.next();
$$;



-- get the JSON version of the STATIONS table
create table citibike.demo.stations_json as (select * from snowflake_demo_resources.citibike_reset_v2.stations_json);

select * from demo.stations_json limit 100;

-- now run the procedure to create STATIONS_VW
call utils.create_view_over_json('demo.stations_json', 'v', 'demo.stations_vw', 'uppercase cols', 'match datatypes');

select * from demo.stations_vw limit 100;
