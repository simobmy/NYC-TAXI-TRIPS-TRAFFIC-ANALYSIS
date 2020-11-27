#!/usr/bin/env python
# coding: utf-8

# **<h1> NYC TAXI TRIPS TRAFFIC ANALYSIS </h1>**

# ** <h2>PART1 : INTRODUCTION </h2>**

# **In this part as a WARM UP I will focus on the preparation of the necessary environment in order to perform the necessary tasks for the analysis and the extraction of the data value. 
# first of all the import of the necessary libraries  the preparation of the environment for massive data processing **

# In[1]:


# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load

# actually the necessary librairy for large scale processing (pyspark) is not provided natively with the notebook provided by kaggle.

get_ipython().system('pip install pyspark')

import numpy as np #  for linear algebra
import pandas as pd # for data processing

#necessary for introducing the work environment
from pyspark import SparkContext
from pyspark.sql import SparkSession

# necessary for doing analysis & evaluations using plots a designing evaluations machine learning models
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error

# necessary for using sql in large scale as a large scale processing purpose.
from pyspark.sql.functions import *
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import DateType, TimestampType, IntegerType
from pyspark.sql.functions import col, lag, unix_timestamp,udf
from pyspark.sql.window import Window

# necessary librairies for time & system management.
from dateutil import parser
import datetime
import os

# You can write up to 20GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All" 
# You can also write temporary files to /kaggle/temp/, but they won't be saved outside of the current session


# **Below are introduced the datasets that i'm gonna work with during my analysis in this notebook**
# *I'll only work with the files in json format because the other files contain data that i judge useless for my problem data analysis*

# In[2]:


for dirname, _, filenames in os.walk('/kaggle/input'):
    for filename in filenames:
        print(os.path.join(dirname, filename))


# **Introducing the environement for work on large scale data processing**

# In[3]:


sc = SparkContext(appName = "MyApplication")
spark = SparkSession.Builder().getOrCreate()


# ** <h2> PART 2 : EXTRACT TRANSFORM LOAD </h2>**

# **In this part i focus on the pre-processing & processing of the datasets all kinds of extracting transformations cleaning to make the data exploitable for the analysis & since it's a data of 1.6GB of information my choice is to do all the process using librairies on python for stocking & processing in a distributed way because it's the efficient and possible way to do that some simple queries on simple dataframe cost me almost 10 minutes & sometimes more so large scale processing here is a must**

# **Load the datasets used for analysis 1.6GB of taxi traffic data in new york**

# In[4]:


nyctaxidata_2009= spark.read.json(r'../input/nyc-trips-taxi-dataset/data-sample_data-nyctaxi-trips-2009-json_corrigido.json')
nyctaxidata_2010= spark.read.json(r'../input/nyc-trips-taxi-dataset/data-sample_data-nyctaxi-trips-2010-json_corrigido.json')
nyctaxidata_2011= spark.read.json(r'../input/nyc-trips-taxi-dataset/data-sample_data-nyctaxi-trips-2011-json_corrigido.json')
nyctaxidata_2012= spark.read.json(r'../input/nyctripstaxidataset/data-sample_data-nyctaxi-trips-2012-json_corrigido.json')


# *Counting the number of lines in each dataset to be sure that the load has done right*

# In[5]:


print(nyctaxidata_2009.count())
print(nyctaxidata_2010.count())
print(nyctaxidata_2011.count())
print(nyctaxidata_2012.count())


# *This cell is designed in purpose to merge the datasets on and unique for analysis*

# In[6]:


nyc_taxi_data = nyctaxidata_2009.union(nyctaxidata_2010.union(nyctaxidata_2011.union(nyctaxidata_2012)))
print(nyc_taxi_data.count())


# In[7]:


nyc_taxi_data


# *Inspecting the columns and types provided in dataset *

# In[9]:


types_data = [t for t in nyc_taxi_data.schema.fields]
types_data


# *Inspecting the number of distinct values for each column in the dataset*

# In[11]:


for column in nyc_taxi_data.columns:
    print(nyc_taxi_data.agg(countDistinct(col(column)).alias("disinct value : "+column)).show())


# 1. > RE : 1 _ Average distance traveled by trips with a maximum of 2 passengers

# **This query written using spark sql to extract the average tripdistance for trips that doesn't exceed 2 passengers **

# In[12]:


nyc_taxi_data.createTempView("taxi_table")
spark.sql("SELECT avg(trip_distance) as AVERAGE FROM taxi_table WHERE passenger_count <=2").show()


# > RE : 2 _  The 3 biggest vendors based on the total amount of money raised

# **This query written using spark sql to show the 3 vendors who made the more money**

# In[13]:


spark.sql("SELECT sum(total_amount) as MONEY from taxi_table GROUP BY vendor_id ORDER BY money DESC LIMIT 3").show()


# *The next 4 cells of code is designed for converting the dates from string format to TIMESTAMPTYPE and show them respectively for preprocessing purpose * 

# In[15]:


udf_myconverter = udf(parser.parse, TimestampType()) 
nyc_taxi_data = nyc_taxi_data.withColumn("dropoff_datetime_converted", udf_myconverter("dropoff_datetime")) 


# In[16]:


nyc_taxi_data.select("dropoff_datetime_converted").show()


# In[17]:


udf_myconverter = udf(parser.parse, TimestampType()) 
nyc_taxi_data = nyc_taxi_data.withColumn("pickup_datetime_converted", udf_myconverter("pickup_datetime")) 


# In[18]:


nyc_taxi_data.select("pickup_datetime_converted").show()


# *Creating a column that describes the date for each trip in the dataset and show it as preprocessing purpose*

# In[19]:


udf_myconverter = udf(parser.parse, DateType()) 
nyc_taxi_data = nyc_taxi_data.withColumn("Trip_date", udf_myconverter("dropoff_datetime")) 


# In[20]:


nyc_taxi_data.select("Trip_date").show()


# *Creating a column that describes the time spended in during the trip from the difference between the pickup time and dropoff time in a trip from calculation and showing it*

# In[21]:


nyc_taxi_data = nyc_taxi_data.withColumn("Trip_time", 
   (unix_timestamp(nyc_taxi_data.dropoff_datetime_converted) - unix_timestamp(nyc_taxi_data.pickup_datetime_converted))
)


# In[22]:


nyc_taxi_data.select("Trip_time").show()


# In[23]:


nyc_taxi_data.select(
    year("Trip_date").alias('year'), 
    month("Trip_date").alias('month'), 
    dayofmonth("Trip_date").alias('day'),
    dayofweek("Trip_date").alias('dayofweek')).show()


# *Verifyin one more time the columns existing in the dataset *

# In[24]:


nyc_taxi_data.columns


#  *Firstly Doing some selections in the trip_date feature & selecting the features needed for the analysis part of the dataset and secondly transforming the spark dataframe into a pandas dataframe to be able to do some analysis in the next part*

# In[25]:


df_plot = nyc_taxi_data.select("Trip_date","Trip_time","fare_amount","pickup_longitude","pickup_latitude","dropoff_latitude","dropoff_longitude","payment_type","tip_amount",year("Trip_date").alias('year'), 
month("Trip_date").alias('month'),dayofmonth("Trip_date").alias('day'),dayofweek("Trip_date").alias('dayofweek'))
pdf_df_plot = df_plot.toPandas()
print(pdf_df_plot.head())


# ** <h2>PART3 : EXTRACTING INSIGHT FROM THE DATA AND ANALYSIS </h2>**

# **In this part i'm focusing doing the processing for the analysis & showing insight with explanations **
# * Plotting results & explanations of the results.
# * Doing the minimal necessary processing on data for memory economy time consuming optimization processing for plotting the results. 
# 
# 
# 

# **Firstly verifying the types of columns after the transformation done**

# In[26]:


pdf_df_plot.dtypes


# **Doing the necessary type transformation & reverifying the columns types in the dataset**

# In[27]:


pdf_df_plot['Trip_date'] = pd.to_datetime(pdf_df_plot['Trip_date'], format='%Y-%m-%d')
print(pdf_df_plot.dtypes)


# # All the processing done in the below 4 cells are designed for a common purpose answer the question : 
# **Make a histogram of the monthly distribution over 4 years of rides paid with cash**

# **Select necessary columns & lower case the payment_type column & doing the filter for a data with CASH payment trips**

# In[28]:


histogram_data = pdf_df_plot[["payment_type", "Trip_date","month","year"]]           
histogram_data["payment_type"] = histogram_data["payment_type"].str.lower()
histogram_data_cash = histogram_data[histogram_data["payment_type"]=="cash"]


# **Grouping for each date & counting the number of trips in each day**

# In[29]:


histogram_data_cash = histogram_data_cash.groupby('Trip_date').size().reset_index(name='count')
histogram_data_cash.head(10)


# *Setting an index for each date*

# In[30]:


histogram_data_cash = histogram_data_cash.set_index('Trip_date')
histogram_data_cash.head(10)


# *Sampling and sum the counting for all the trips for each month*

# In[31]:


e = histogram_data_cash['count'].resample('MS').sum()
e


# > RE : 3 _Doing some analysis using ahistogram of the monthly distribution over 4 years of rides paid with cash;

# *Plotting a bar for each month in the 4 years*

# # **By plotting the density of trips for each month during the 4 years of provided data we can exactly from the plot observe 4 Gaussian distributions due to the seasonal behavior of taxi_trips traffic in new-york and a trend that increment a little bit after each year**

# In[32]:


e.plot(kind="bar")


# # All the processing done in the below 7 cells are designed for a common purpose answer the question : 
# **Make a time series chart computing the number of tips each day for the last 3 months of 2012**

# *Select necessary feature for the problem*

# In[36]:


data_tip_chart = pdf_df_plot[["Trip_date","tip_amount"]]


# *Making a group of trips in each day and summing the tip_amounts*

# In[37]:


data_tip_chart = data_tip_chart.groupby('Trip_date')['tip_amount'].sum().reset_index()


# *Setting an index for Trip_date*

# In[38]:


data_tip_chart = data_tip_chart.set_index('Trip_date')


# *Verifying if the index is done*

# In[39]:


data_tip_chart.index


# *Sampling tip_amounts for each business day and selecting the 3 last month of 2012 for the analysis*

# In[40]:


y = data_tip_chart['tip_amount'].resample('B').mean()
y=y['2012-8-1':]


# *Verifying*

# In[41]:


y


# > RE : 4 _Make a time series chart computing the number of tips each day for the last 3 months of
# 2012.

# # During the last 3 month of 2012 in business days the traffic is quite stable on a mean value. 

# *Plotting the designed data using matplotlib*

# In[42]:


import matplotlib.pyplot as plt
y.plot(figsize=(15, 6))
plt.show()


# > RE : BONUS 1  _The average trip time on Saturdays and Sundays

# # 524.95seconds is the average time trips in saturdays & sundays

# In[43]:


pdf_df_plot[(pdf_df_plot.dayofweek == 6) | (pdf_df_plot.dayofweek == 7)]["Trip_time"].mean()


# # All the processing done in the below 7 cells are designed for a common purpose answer the question : 
# **Analyzing the data to find and prove seasonality**

# In[45]:


data_seasonality = pdf_df_plot[["Trip_date","tip_amount"]]


# In[46]:


data_seasonality = data_seasonality.groupby('Trip_date').size().reset_index(name='count')


# In[47]:


data_seasonality.head(10)


# In[48]:


data_seasonality = data_seasonality.set_index('Trip_date')


# In[49]:


print(data_seasonality.index)
print(data_seasonality.head(20))


# In[50]:


z = data_seasonality['count'].resample('MS').mean()
z


# > RE : BONUS 2  _Proving the seasonality of the data

# # **By plotting the number of trips monthly during the 4 years of provided data i can observe the same seasonality repeating 4 times the exact number of years in the data and also the number of trips that's more & more after each year that prove the trend**

# In[51]:


z.plot(figsize=(15, 6))
plt.show()


# > RE : BONUS 3  _Creating assumptions using machine learning models

# # I'm doing some processing on the data to prepare it to be fitted into the model and tested by the same model*
# # The data i'm using is actually the quantity of trips for each month in the 4 years so each value between the 45 value used in the problem is a value that describe the quantity of trips in a specific month, for the machine learning problem i chosen 29 value from the data for training the model & only 16 for testing the model. 

# In[74]:


print(len(X))
print(len(train),len(test))


# In[71]:


X = z.values
X = X[np.logical_not(np.isnan(X))]
size = int(len(X) * 0.66)
train, test = X[0:size], X[size:len(X)]
history = [x for x in train ]
predictions = list()
for t in range(len(test)):
    model = ARIMA(history, order=(5,1,0))
    model_fit = model.fit(disp=0)
    output = model_fit.forecast()
    yhat = output[0]
    predictions.append(yhat)
    obs = test[t]
    history.append(obs)
    print('predicted=%f, expected=%f' % (yhat, obs))
error = mean_squared_error(test, predictions)

print('Test MSE: %.3f' % error)
# plot
pyplot.plot(test)
pyplot.plot(predictions, color='red')
pyplot.show()


# # The 9250.824 is a good MSE for validation by testing i observe that the model do a good regression & the plot shows, We can see the values show some trend and are in the correct scale & seasonality

# ** <h2>PART4 : CONCLUSION </h2>**

# **The main purpose of this analysis is being able to extract the insight from data in the most optimized way in term of time & memory costs because it's a large scale processing of data and each query can cost money and cost in global warming**
# **The crucial strategies that needed for doing a good management of the nyc taxi traffic
# Giving a heat map of new_york taxis based on the fare_amount of taxis
# Show the seasonality of the traffic and trying forecast it 
# a Heat map of latitude and longitude map view of pickups and dropoffs in the year**
# 
# *Those assertions gonna help deciders manage the trafic of taxis for more confortable experience in the city & also preserving the world from global warming*
# *And also taxi customers to be aware of the taxi prices depending on locations*
