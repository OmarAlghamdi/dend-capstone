# Capstone Project
This is the final project in Udacity's Data Engineering Nanodegree.

In this project I put what I learned through the namenodegree into work. I acquired date, analyzed it and applied an ETL process to it using Spark to enable business analysis on the date.

## Source Data
Source data are bike-share data located in the Bay Area, CA. the original data is in csv format and it consist of 4 parts
- `station.csv` 70 rows
- `status.csv` ~72 million rows
- `trip.csv` ~670 thousand rows
- `weather.csv` 3665 rows

I also added additional CSV file that maps cities to zip codes to allow analysis based on weather data in specific city.

The data is about 2GB in size and hosted on public S3 bucket `omar-dend`. the region of the bucket is `us-west-2`. The data was taking from Kaggle, you can find them [here](https://www.kaggle.com/benhamner/sf-bay-area-bike-share)

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

## Data Quality
The data is in high quality, no cleaning is needed. There are few measurements related to weather are missing from the data (few cells).

Duplicate datetime data are dropped from the time table during ETL process

## Data Model
The data is after executing the ETL pipeline will be in dimensional model form. There are 3 dimensional tables and 2 fact tables.
![dimensional model](https://github.com/OmarAlghamdi/dend-capstone/blob/master/out/model.png)

## The ETL Pipeline
Since the data is in CSV format, stored in S3 and required date & time  extraction and transformation from the all 4 csv files from all . I chose Spark to do the job because 
- Sparks's dataframes are suitable for the transformation job.
- Furthermore, the fact and dimension table are loaded back to S3 in praquet format.
- The advantage of this is cost saving since the AWS EMR Spark cluster needs to be running only during the ETL process. After that it can be terminated to save cost.
- Finally, reading data from S3 into spark dataframe and do the transformation there is faster than copying the data to Redshift and transform it and load it into dimensional tables using SQL queries.

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

## Constricts Regrading Production
Here are few thins you should not about the ETL pipeline. These notes are based on 'Step 5' of the project instructions.

### Goals and Queries
As I mentioned above, there are many business related analysis to can be done on the data. Here are few examples:
- understand bike usage pattern through the day to account for peaks in demand
- understand where the majority of trips happen to plan future dock station expansions
- understand bike usage patterns related to weather conditions

### Pipeline Automation
The pipeline can be run step by step through the available notebook. But in production environment the python script should be used to save engineer's time

### Scenarios
- If the data was increased by 100x.
  
  Since the data is extracted from S3 and loaded back to S3, data can increase infinity without any space issues. The only resources will be required to expand overtime is the size and the number of Spark node in a cluster. but since the EMR cluster is only active during the runtime or the ETL process it should not be an Issue

- If the pipelines were run on a daily basis by 7am.
  
  AWS EMR Spark cluster creation and teardown can be automated through IaC. Data Engineer overseeing the process can use AWS Lambda Functions to trigger cluster creation at 7am each day, run the python script, then teardown the cluster of save money.

- If the database needed to be accessed by 100+ people.
  
  Sine the dimensional tables are stored in S3, and S3 is scalable managed service, data engineer should not worry increase in users. Different users can use different BI tools to read the data from S3 without any issue
