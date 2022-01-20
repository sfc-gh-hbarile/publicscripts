# Credit Card Fraud Detection Demo
This is a demo is based on the Machine Learning for Credit Card Fraud detection - Practical handbook, https://fraud-detection-handbook.github.io/fraud-detection-handbook/

It shows how to do Feature Enginerring with Snowpark, preparing data for training a Machin Leraning model and finaly how to deploy and use a trained model in Snowflake using Python UDF.

### PRE-REQUISITE

1. Make sure you have a Snowflake account that can use Snowpark for Python and Python UDFs
2. Install Snowpark for Python according to the documentation
3. The demo also use the following Python libraries that is not part of a standard Python installation, so you need to make sure they are avalible:
   ```
   json
   sklearn
   pandas
   numpy
   ```
3. Install Jupyter or JupyterLab

### SETUP

1. Open terminal and clone this repo or use GitHub Desktop, since it is part of the snowflakecorp organisation you need to set up the authentification before cloning: 

    `git clone https://github.com/Snowflake-Labs/snowpark-python-examples`

2. Change to the `Credit Card Fraud Detection` directory and launch  JupyterLab

    `jupyter lab`

6. Paste the URL in a browser window and once JupyterLab comes up, switch to the work directory and update `creds.json` to reflect your snowflake environment.

In order to load data you can either run the `00 - Snowpark Python - Load Data.ipynb` notebook.

Or "manual" load it by following the steps below
1. In your snowflake account create the following table:

```
create or replace TABLE CUSTOMER_TRANSACTIONS_FRAUD (
 TRANSACTION_ID NUMBER,  
 TX_DATETIME TIMESTAMP_NTZ, 
 CUSTOMER_ID NUMBER, 
 TERMINAL_ID NUMBER, 
 TX_AMOUNT FLOAT, 
 TX_TIME_SECONDS NUMBER, 
 TX_TIME_DAYS NUMBER, 
 TX_FRAUD NUMBER, 
 TX_FRAUD_SCENARIO NUMBER);
```

3. Load the data/fraud_transactions.csv.gz into CUSTOMER_TRANSACTIONS_FRAUD

4. After loading the CUSTOMER_TRANSACTIONS_FRAUD table generate the CUSTOMERS and TERMINALS tables using the following SQL

```
CREATE TABLE CUSTOMERS
AS
 SELECT DISTINCT CUSTOMER_ID FROM CUSTOMER_TRANSACTIONS_FRAUD
  ORDER BY CUSTOMER_ID;
```

```
 CREATE TABLE TERMINALS
 AS
 SELECT DISTINCT TERMINAL_ID FROM CUSTOMER_TRANSACTIONS_FRAUD
  ORDER BY TERMINAL_ID;
```



### DEMO
The demo talk track is the noteboks.

Start with 01 - Snowpark Python - Feature Engineering


