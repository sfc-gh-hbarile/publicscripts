# StyleMeUp - Fraud Detection in Online Retail 

#### Problem Description: 
A global retailer 'StyleMeUp' has been experiencing transaction fraud. To reduce costs related to fraudulent transactions, StyleMeUp wants to implement a fraud detection solution that leverages machine learning. 

This demo showcases how Data Engineering and Data Science teams at StyleMeUp can use familiar programming concepts and APIs, and a rich ecosystem of open source packages provided by Snowpark for Python to collaborate and build this solution.

Some cool features in this demo are -

1. Using Snowflake native GEOGRAPHY datatypes and ST_DISTANCE geography functions to calculate ip_to_shipping distance. (No need for GeoPandas)
2. Load data using pandas data frame (new functionality in Snowpark Python API)
3. Create and deploy UDF in snowflake without pickle
4. Using scikit learn, pandas, NumPy 

Steps to recreate this demo are simple.

1. load the notebooks in your development environment
2. place the unzipped data files "orders.csv" and "order_details.csv" in the same dir as the notebooks
3. update config.py file with your snowflake account details
4. run all the notebooks starting from 00_cc~ to 02_cc~

#### Note* You will need the IPINFO Privacy marketplace dataset. You can request it and mention that you are a snowflake employee and need this for customer demo. It gets enabled for your account within few hours