# NYC-TAXI-TRIPS-TRAFFIC-ANALYSIS

** <h2> MOTIVATIONS </h2>**
The typical taxi taffic faces a common problem of taxi taffic management &amp; efficiently assigning the cabs to passengers so that the service is smooth and hassle free.

** <h2>ABOUT THE DATA </h2>**
*The provided*
<span>data-vendor_lookup-csv.csv</span>
 *data-payment_lookup-csv.csv*
*data-sample_data-nyctaxi-trips-2009-json_corrigido.json*
*data-sample_data-nyctaxi-trips-2011-json_corrigido.json*
*data-sample_data-nyctaxi-trips-2010-json_corrigido.json*
*data-sample_data-nyctaxi-trips-2012-json_corrigido.json*

*Data fields used for doing my analysis*
*vendor_id - a code indicating the provider associated with the trip record*
*pickup_datetime - date and time when the meter was engaged*
*dropoff_datetime - date and time when the meter was disengaged*
*passenger_count - the number of passengers in the vehicle (driver entered value)*
*pickup_longitude - the longitude where the meter was engaged*
*pickup_latitude - the latitude where the meter was engaged*
*dropoff_longitude - the longitude where the meter was disengaged*
*dropoff_latitude - the latitude where the meter was disengaged*
*trip_distance - duration of the trip in seconds*
*fare_amount - the tarif of taxi ride*
*Tip_amount - the tip of the fare*
*Tolls_amount - *
*Total_amount - the total money *
*Type_payment - Describes the type of the payment*
** <h2>PART1 : INTRODUCTION </h2>**
*In this part as a WARM UP I will focus on the preparation of the necessary environment in order to perform the necessary tasks for the analysis and the extraction of the data value. 
first of all the import of the necessary libraries  the preparation of the environment for massive data processing *

** <h2> PART 2 : EXTRACT TRANSFORM LOAD </h2>**

*In this part i focus on the pre-processing & processing of the datasets all kinds of extracting transformations cleaning to make the data exploitable for the analysis & since it's a data of 1.6GB of information my choice is to do all the process using librairies on python for stocking & processing in a distributed way because it's the efficient and possible way to do that some simple queries on simple dataframe cost me almost 10 minutes & sometimes more so large scale processing here is a must*

** <h2>PART3 : EXTRACTING INSIGHT FROM THE DATA AND ANALYSIS </h2>**

*In this part i'm focusing doing the processing for the analysis & showing insight with explanations *
* Plotting results & explanations of the results.
* Doing the minimal necessary processing on data for memory economy time consuming optimization processing for plotting the results. 

** <h2>PART4 : CONCLUSION </h2>**

*The main purpose of this analysis is being able to extract the insight from data in the most optimized way in term of time & memory costs because it's a large scale processing of data and each query can cost money and cost in global warming**
*The crucial strategies that needed for doing a good management of the nyc taxi traffic
Giving a heat map of new_york taxis based on the fare_amount of taxis
Show the seasonality of the traffic and trying forecast it 
a Heat map of latitude and longitude map view of pickups and dropoffs in the year*

*Those assertions gonna help deciders manage the trafic of taxis for more confortable experience in the city & also preserving the world from global warming*
*And also taxi customers to be aware of the taxi prices depending on locations*


