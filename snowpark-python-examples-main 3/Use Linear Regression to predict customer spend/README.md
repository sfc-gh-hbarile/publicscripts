This is an example of using Linear Regression model in Scikit-Learn, Snowpark and Python UDFs to predict customer spend. The training of the model happens locally while the scoring is done within Snowflake using the UDF created via Snowpark in the Jupyter Notebook. The sample data file is EcommerceCustomers. Model training is done on a test dataset while scoring is done on the entire dataset. 

## Use Case

An ecommerce retailer is looking to use machine learning to understand its customer's online engagement with its digital outlets i.e website and app. It is trying to decide whether to focus its efforts on the mobile app experience or website. We will use Linear Regression model to see which user acitivity has the biggest impact on their likelyhood of spending more money.

Variables of interest:

Avg. Session Length: Average session of in-store style advice sessions.
Time on App: Average time spent on App in minutes
Time on Website: Average time spent on Website in minutes
Length of Membership: How many years the customer has been a member 

If any of the packages used in the example are not part of your python environment, you can install them using <br>
*import sys <br>
!conda install --yes --prefix {sys.prefix} <package_name>*
