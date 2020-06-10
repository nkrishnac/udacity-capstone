# Project Description:

- As more and more immigrants come to the US for diffrent purposes, government agencies have the need to access this imformation quickly using reliable methods. Understanding this data can help politicians to reform the immigration so that they can bring in young and creative people from all the around the world which can help boost the US economy.

- To facilitate this, we have raw data available from different sources which are listed below. Using these we will transform and create a datalakes is S3 using pyspark and then load them to table is Redshift which can be queried to do analysis such as:

    - Average age, Average Household size, race of the immigrants
    - Top 10 states where Immigrants are moving to
    - Are average temperatures in US cities impacting the immigrants decision to move to the city
    - Number of immigrants moving to the year per month/year
    - Are people coming in to visit or to live in? What is the most popular visa?
    - Top 10 famous tourist destinations in US
    - And many more....
    
# Data Sources:
- I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. 
- I94_SAS_Labels_Descriptions.SAS: labels explaining about the data in the above files are explained [here](https://github.com/Ioana-Postolache/US-Immigration/blob/master/I94_SAS_Labels_Descriptions.SAS)
- World Temperature Data: This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
- U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
- Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).

# Explore and Assess the data:
- Found dupliactes and double quote characters in the raw data.
- Issues like this and few other have been taken care while transforming and loading the data into S3 datalake

# Data Model:
- Raw data is read, transformed and loaded into S3 data lake using pyspark. pySpark is used because:
    - In-memory Computation in Spark
    - Swift Processing
    - Fault Tolerance in Spark
- S3 Provides an optimal foundation for data lake because of its virtually unlimited scalability. Storage can be increased from gigabytes to petabytes seamlessly and nondiruptively. Also S3 is designed to provide 99.999999999% durability
- Apache airflow is used as a workflow scheduler to load the data from S3 data lake to Amazon Redshift
    - The most important advantage of Apache Airflow is that it provides the power of scheduling the analytics workflow and - - Data warehouse also managed under a single roof so that a comprehensive view accessed to check the status.
    - The logs entries of execution concentrated at one location.
    - The use of Airflow also matters as it has a strength to automate the development of workflows as it has an approach to configure the workflow as a code.
    - It can also give a reporting message through slack if an error comes due to the failing of DAG.
    - Within the DAGs, it provides a clarion picture of the dependencies.
    - The ability to generate the metadata gives an edge of regenerating distinctive uploads.
- Amazon Redshift is used as datawarehouse for our analytical purposes because of it's obvious reasons such as:
    - Exceptionally fast, gains high performance using massive parallelism, effidient data compression, query optimization and distribution. 
    - Also it is horizontally scalble with massive storage capacity and transparent pricing. 
    - Redshift provides SQL interface and works with exisitng Postgres JDBC/ODBC drivers readily connecting to most of the Business Intelligence tools.
    - Since we are using S3 as datalake in our use case, Redshift can access formatted data on S3 with single copy command.

# Prerequisites:
- Create an S3 bucket and a redshift cluster in the same region
- Upload the raw data sets to S3 bucket to be read by pyspark and load them back to the S3 datalake

# Data Dictionary:
- Following tables are created in Amazon Redshift using Apache airflow DAGs:
- dim_city: built on city code data from raw airport and demographics data
    - city: Name of the city
    - state_code: State in which the City is in
    - city_id: Unique Identifier to identify the city
- dim_airport_codes: built on raw airport data, filtered for US airports, joined with dim_city table to get city_id
    - icao_code: Unique Identifier for airport
    - type: Type of airport
    - name: Name of the airport
    - elevation_ft: Elevation above sea level
    - airport_latitude: Latitude
    - airport_longitude: Longitude
    - city_id: to Identify which city the airport in (populated using dim_city)
- dim_us_cities_demographics: built on raw demographics data
    - median_age: Median Age of Immigrants moving to the city
    - average_household_size: Average size of the household
    - race: Immigrant's race
    - male_population: Number of male immigrants in the city
    - female_population: Number of female immigrants in the city
    - total_population: Total number of immigrants in the city
    - num_veterans: Number of veterans
    - foreign_born: US Citizens born on foreign land
    - city_id: to Identify city (populated using dim_city)
- dim_us_cities_temperatures: built on global weather data, filtered for US cities, joined with dim_city table to get city_id
    - avg_temperature: Average temperature in the city over a period of time
    - std_temp: Average Temperature Uncertainty
    - city_id: to Identify city (populated using dim_city)
- dim_airports_weather: Joining weather data with airport location to get historical and latest weather info for airports
    - name: Name of the airport
    - elevation_ft: Elevation above sea level
    - city: City the airport is in
    - state: State the airport is in
    - avg_temp: Average temperature at the airport/in the city
    - std_temp: Average Temperature Uncertainty
- fact_immigrant_info: Information about individual immigrants built using I94 Immigration dataset
    - cicid: Unique Identifier
    - from_country_code: Immigrant's resident Country
    - age: Age of the Immigrant
    - visa_code: Visa on which Immigrant cam to the US
    - visa_post: Consulate at Visa is Issued at
    - occupation: Occupation of the Immigrant
    - visa_type: Type of the visa
    - birth_year: Year of Immigrant's birth
    - gender: Non-immigrant sex
    - arrival_date: Date arrived into the US
    - arrival_month: Month arrived into the US (to analyse most popular tourist months based on visa_type)
    - arrival_year: Year arrived into the US
- fact_immigration_info: nformation about immigration information, such as date of arrival, visa expiry date, airport, means of travel
    - cicid: Unique Identifier
    - iata_code: Secondary Identifier
    - state_code: State in which  immigrant resides
    - airline: airline used to arrive in the US
    - admnum: I94 Admission Number
    - fltno: Airline Flight Number
    - arrival_flag: Arrival Flag - Admitted or paroled into the US
    - arrival_date: Date arrived into the US
    - departure_date: Departure Date from the US
    - allowed_until: Date to which immigrant is allowed to stay until
    - arrival_month: Month arrived into the US (to analyse most popular tourist months based on visa_type)
    - arrival_year: Year arrived into the US
- fact_immigration_demographics: Joining immigration with demographics data (Analyse cities which immigrants are interested to move to, top 10 favorite tourist dstinations in the US and so on)
    - cicid: Unique Identifier
    - age: Age of the Immigrant
    - gender: Non-immigrant sex
    - arrival_month: Month arrived into the US
    - arrival_year: Year arrived into the US
    - from_country: Immigrant's resident Country
    - state: State in which  immigrant resides
    - median_age: Median Age of Immigrants moving to the city
    - total_population: Total number of immigrants in the city
    - foreign_born: US Citizens born on foreign land

# ETL Steps:
- Raw data sources are uploaded to S3 bucket and are read, transformed and laoded back to s3 datalake usong pyspark.
- Run the below Jupyter notebook cells step by step:
    - Data Engineer Capstone Project.ipynb
- Once the datalake is created, run the airflow dag which populates data from S3 to Amazon Redshift
    - DAG: us_immigration.py
- Monitor the DAG to error/completion in Airflow UI (DAG sample shown below)
    ![alt text](![Airflow - US_Immigration_DAG](https://user-images.githubusercontent.com/23348082/84319143-891dae80-ab3d-11ea-8df1-e77ff59f49ca.png))
# Issues faced:
- While laoding from S3 parquet files to Redshift, getting spectrum scan error saying unmatched columns between table and data file. Data in parquet files in S3 is fine and I couldn't figure out a solution for this even after lots of trouble shooting. Hence I loaded the raw data to S3 in csv format and then to redshift.
- Similar to above, even when parquet files are having data, nothing is loaded into redshift (dim-airport had this issue)
- Airflow DAGs are not getting refreshed after renaming the DAG file. Apparently found out this is a bug in Airflow which is yet to be fixed. 

# Schedule to run the ETL:
- S3 datalake can be run when the raw files are available, let's say daily at 3 AM
- However, do not run the Airflow tasks until all the raw files are processed as they are dependent on each other.
- Monitor the processes until completion so not to miss the SLA.

# Future considerations and Improvements:
- If the data manifolds 100 times - Instead of running spark session on few EC2 instances, create a cloud formation stack to spin up EMR cluster and create spark session to handle the volume. Also Airflow can be installed on the EMR cluster. Once the pipeline is completed, terminate the cluster for cost optimization. 
