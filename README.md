# parking
Parking app - functions
This repository contains code of Time Triggered Azure function which is used within project focused on estimation of number of free parking spaces within monitored garage. 

**Detailed function flow:**

1. Obtaining and processing data from *yr.no* - method GetWeatherData(), API address and request parameters are defined by variable *uri*

2. In next step we save obtained weather forecast data to database using SQLBulkCopy. To build query in easier way we use *FastMember nuget*. Data are being saved for further model training and evaluation purposes.

3. Subsequently we create csv file, which is further used to invoke batch request trained Azure Machine Learning model published as a web service. It contains parameters needed to predict number of free spaces for next 48 hours with 5 minute granularity. These parameters are mixture of collected weather forecast data and historic data obtained from database.

4. When we have created csv. file we invoke batch execution. These consist from following steps:

   - copy csv file to Azure Storage container
   - fire request to Azure ML Web Service
   - regularly check status of execution
   - when finished, ML Web Service outputs result to the same Azure Storage blob container
   - we read the file and save results to database

5. In last step function reschedules itself to run again, when new forecast is available. This process is described below


**Function schedule:**

Function is being run every time the weather forecast is refreshed. We reschedule it based on time information about next update of forecast, which is obtained from yr.no API. To do so, we rewrite function.json file specifically schedule line. Code can be find in  RescheduleTimeTrigger method. Be aware that modifying function.json causes Function App to restart, so when it can effect other functions defined within Function App.



**In order to run this function you need to:**

1. Create your Machine Learning model
2. Modify creation of csv file for batch execution according to inputs required by your ML model
3. Set the keys, URIs and connection strings within App Service Settings namely:
   - Connection Strings:
     - sqldb_connection (Connection string to db)
   - App Settings:
     - mlEndpoint (Machine Learning endpoint without version parameter specified)
     - storageName (Name of storage account)
     - storageKey (Key of storage account)
     - mlApiKey (Key for Machine Learning endpoint)





