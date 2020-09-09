# Capstone Project
This is the final project in Udacity's Data Engineering Nanodegree.

In this project I put what I learned through the namenodegree into work. I acquired date, analyzed it and applied an ETL process to it using Spark to enable business analysis on the date.

## Step 1:
## Source Data
Source data are bike-share data located in the Bay Area, CA. the original data is in csv format and it consist of 4 parts
- `station.csv` 70 rows
- `status.csv` ~72 million rows
- `trip.csv` ~670 thousand rows
- `weather.csv` 3665 rows

I also added additional CSV file that maps cities to zip codes to allow analysis based on weather data in specific city.

The data is about 2GB in size and hosted on public S3 bucket `omar-dend`. the region of the bucket is `us-west-2`. The data was taking from Kaggle, you can find them [here](https://www.kaggle.com/benhamner/sf-bay-area-bike-share)

The purpose of carrying ETL process is to prepare the data for business analyses to help the bike share company understand demand & patterns through the Bay Area and be able to take data driven decision when it come to expansions, marketing, and operation in general. 

## Step 2
## Data Quality
The data is in high quality, no cleaning is needed. There are few measurements related to weather are missing from the data (few cells).

Duplicate datetime data are dropped from the time table during ETL process

### Station
Station data is about bike docking stations. It consists of 7 columns
- `id`: int, station id
- `name`: string, station name
- `lat`: float, station latitude
- `long`: float, station longitude
- `dock_count`: int, number of docks in the station
- `city`: string, name of the city where the station is
- `installation_date`: string, date in the format MM/dd/yyyy

### Status
Status data is about the status of the docking stations through time. It consists of 4 columns
- `station_id`: int, station id
- `bikes_available`: int, number of bikes in the station
- `docks_available`: int, number of empty docks
- `time`: staring time & date in the format yyyy/MM/dd HH:mm:ss

### Trip
Trip data is about the trips taking by customers. It does not include any costumer identifiable information. It consists of 11 columns
- `id`: int, trip id
- `duration`: int, trip duration is seconds
- `start_date`: string time in minutes and date in the format MM/dd/yyyy HH:mm
- `start_station_name`: string, station name
- `start_station_id`: int, station id
- `end_date`: string time in minutes and date in the format MM/dd/yyyy HH:mm
- `end_station_name`: string, station name
- `end_station_id`: int, station id
- `bike_id`: int, bike id
- `subscription_type`: string, either 'Subscriber' or 'Customer'
- `zip_code`: int, zip code

### weather
Weather data is about different weather measurements of each city each day. they are identified by zip code. It consists of 24 columns. To save the space here you can details about the different measurements in [Kaggle](https://www.kaggle.com/benhamner/sf-bay-area-bike-share?select=weather.csv). The main measurements that are used later in the dimensional model are related to temperature, humidity, wind speed & precipitation

### City
City data is just a map between the zip codes and city names in the data. It consists of 2 columns
- `city`: string, city name
- `zip_code`: int, zip code

## Step 3
## Use Cases & Data Model Choice
The data is modeled as Star Schema with dimension and fact tables.

The reason this model is chosen is to ease business analysis on the data and allow different Bi tools to used based on the company needs (e.g. marketing team vs. operation team). Examples of the question can be asked on the data are:
- How demand differ on weekdays vs. weekend?
- Which dock stations get the most traffic?
- Where from and to where riders go?
- How weather (temperature, humidity or rain) affect ridership?
- Which stations are overladed and should get expansion?
- Which stations are not profitable?

The data is after executing the ETL pipeline will be in dimensional model form. There are 3 dimensional tables and 2 fact tables.
![dimensional model](https://github.com/OmarAlghamdi/dend-capstone/blob/master/out/model.png)

For more details about each filed see the data dictionary at the end of this page.

## Step 4
## Tools Choice
- Spark: spark.sql functions and dataframe is better in performing the data transformation for this project than using pure SQL queries with, say, AWS Redshift. The main reason for that SQL queries are not flexible enough to handle the datetime extraction transformation transformation from multiple CSV files to one dimensional table. Each CSV file had it one date format. Additionally, An AWS EMR Spark cluster can be booted during transformation, does not happen frequently, then terminated to save cost
- S3: S3 is elastic storage service, If the bike share business grow largely in the future, S3 will be able to handle terabytes of data with out need for data migration or to increase database cluster size. Additionally, Storing dimensional tables on S3 is cheaper than always on database cluster, such as AWS Redshift.
- AWS EMR Cluster for Spark: The advantage of using EMR rather than EC2 instance or other method is that EMR is pre-configured and ready to run spark jobs. A cluster can boot quickly and shutdown after ETL process complete. Using AWS EMR makes it easier in the future to automate the entire ETL process by scheduling AWS Lambda Functions to create a cluster using Infrastructure as Code IaC and submit a spark job to it.
- Jupyter Notebook: the ETL process has small distinct steps to Extract, Transform & Load the data. using a notebook make more readable allow one step rerun if it fails. I also converted it into a script that can be submitted to a spark cluster

## How To execute The ETL Process
It is recommended to run the ETL process in the notebook:
- Create an AWS EMR Spark cluster. 3 or more instances of size `m5.xlarge` is the recommended cluster size to handle the data processing
- If you are running this ETL process after September 10, 2020 you will need to copy the data from Kaggle (see link above) to your S3 bucket.
- create upload the notebook to AWS EMR to have the ease of running the ETL processes without connection adn permission configurations.

If you want to run the script:
- create a cluster save as above
- ssh into the cluster
- copy the script to the cluster
- submit a spark job to execute the script

## Step 5
## Constricts Regrading Production
The data model was designed to allow flexible queries to run in order to be able to answer business related questions (some are provided in step 3).

### Pipeline Automation
Data should be updated at least weekly in this hypothetical scenarios. But since there is no data API or another way to inject data frequently to S3 I left this one. If, say, ETL process would run once a week Data Engineer can afford to run it manually. But down the road Lambda Functions can be used to automate the runs of the python script.

Airflow was not used because data is stored on S3 rather and HDFS. Keeping spark cluster running for long time is expensive when it only used for computation. Additionally, Airflow requires a instance to be running all the time time schedule the jobs. While the same job can be achieved down the road through scheduled AWS Lambda Functions to spin a spark cluster using IaC and to execute the ETL script

## Scenarios from The Instructions

- If the data was increased by 100x.
  
  Since the data is extracted from S3 and loaded back to S3, data can increase infinity without any space issues. The only resources will be required to expand overtime is the size and the number of Spark node in a cluster. but since the EMR cluster is only active during the runtime or the ETL process it should not be an Issue

- If the pipelines were run on a daily basis by 7am.
  
  AWS EMR Spark cluster creation and teardown can be automated through IaC. Data Engineer overseeing the process can use AWS Lambda Functions to trigger cluster creation at 7am each day, run the python script, then teardown the cluster of save money.

- If the database needed to be accessed by 100+ people.

  In order to be able to provide the dimensional table to more users in the same time, the tables should reside in a cluster with high computation power and bandwidth. Since I used Spark to perform the ETL, CPU power is only required during the process. After that, end users can access the dimensional tables directly from S3. Since S3 is and elastic service, it can manage large simultaneous read request in the same time. End users can point the BI tool to pull data from S3 directly

## Data Dictionary
`dim_station`
| Field                 | Type     | Constraint  | description |
| --------------------- | -------- | ----------- | ----------- |
| station_id            | int      | primary key |             |
| station_name          | string   |             |             |
| lat                   | float    |             | latitude    |
| long                  | float    |             | longitude   |
| dock_count            | int      |             |             |
| city                  | string   |             |             |
| installation_datetime | datetime |             |             |

`dim_time`
| Field    | Type     | Constraint  | description       |
| -------- | -------- | ----------- | ----------------- |
| datetime | datetime | primary key |                   |
| second   | int      |             |                   |
| minute   | int      |             |                   |
| hour     | int      |             |                   |
| day      | int      |             |                   |
| week     | int      |             | week of the year  |
| month    | int      |             |                   |
| year     | int      |             |                   |
| weekday  | int      |             | number of the day |

`dim_weather`
| Field                | Type     | Constraint              | description                                |
| -------------------- | -------- | ----------------------- | ------------------------------------------ |
| datetime             | datetime | primary key (composite) |                                            |
| max_temperature_f    | int      |                         | temperature in fahrenheit                  |
| mean_temperature_f   | int      |                         | temperature in fahrenheit                  |
| min_temperature_f    | int      |                         | temperature in fahrenheit                  |
| max_humidity         | int      |                         | from 0 to 100                              |
| mean_humidity        | int      |                         | from 0 to 100                              |
| min_humidity         | int      |                         | from 0 to 100                              |
| max_wind_Speed_mph   | int      |                         |                                            |
| mean_wind_speed_mph  | int      |                         |                                            |
| precipitation_inches | int      |                         | if it rained                               |
| events               | string   |                         | status of the weather e.g. rain, fog, etc. |
| zip_code             | int      | primary key (composite) |                                            |

`fact_trip`
| Field             | Type     | Constraint  | description                |
| ----------------- | -------- | ----------- | -------------------------- |
| trip_id           | int      | primary key |                            |
| duration          | int      |             | duration in seconds        |
| bike_id           | int      |             |                            |
| subscription_type | string   |             | Subscriber or Customer     |
| start_station_id  | int      | foreign key |                            |
| end_station_id    | int      | foreign key |                            |
| datetime_start    | datetime | foreign key | trip start time in minutes |
| datetime_end      | datetime | foreign key | trip end time in minutes   |



`fact_status`
| Field           | Type     | Constraint  | description                 |
| --------------- | -------- | ----------- | --------------------------- |
| station_id      | int      | primary key |                             |
| bikes_available | int      |             |                             |
| docks_available | int      |             | empty docking spaces        |
| datetime        | datetime | foreign key | time when data was captured |




