from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def create_spark_session():
    """
    Creates a new Spark session and retrun it.

    The Spark session is configured to connect to AWS S3
    """
    spark = SparkSession \
        .builder \
        .appName("capstone ETL") \
        .getOrCreate()
    return spark


def process_data(spark):
    """
    Read from S3 and process bike share data into dimensional tables.
    
    the bike share data (as CSVs) is read from a public S3 bucket to dataframes.
    the data is transformed using pyspark.sql functions
    finally data is saved back to the same S3 bucket in parquet fromat
    
    Parameters:
        spark: Spark session    
    """

    # read from S3 to dataframes
    st_station_df = spark.read.csv('s3://omar-dend/station.csv', header=True)
    st_weather_df = spark.read.csv('s3://omar-dend/weather.csv', header=True)
    st_trip_df = spark.read.csv('s3://omar-dend/trip.csv', header=True)
    st_status_df = spark.read.csv('s3://omar-dend/status.csv', header=True)
    st_city_df = spark.read.csv('s3://omar-dend/city.csv', header=True)

    # save counts to ensure later that all rows are present
    station_count = st_station_df.count()
    weather_count = st_weather_df.count()

    # adding timestamp to all the dataframes to standardize datetime
    st_station_df = st_station_df.withColumn('datetime', F.to_timestamp(st_station_df.installation_date, 'MM/dd/yyyy'))

    st_weather_df = st_weather_df.withColumn('datetime', F.to_timestamp(st_weather_df.date, 'MM/dd/yyyy'))

    st_trip_df = st_trip_df.withColumn('datetime_start', F.to_timestamp(st_trip_df.start_date, 'MM/dd/yyyy HH:mm'))
    st_trip_df = st_trip_df.withColumn('datetime_end', F.to_timestamp(st_trip_df.end_date, 'MM/dd/yyyy HH:mm'))

    st_status_df = st_status_df.withColumn('datetime', F.to_timestamp(st_status_df.time, 'yyyy/MM/dd HH:mm:ss'))

    # create dim_weather
    weather_df = st_weather_df.select('max_temperature_f', 'mean_temperature_f', 'min_temperature_f',
                                      'max_humidity', 'mean_humidity', 'min_humidity',
                                      'max_wind_Speed_mph', 'mean_wind_speed_mph',
                                      'precipitation_inches',
                                      'events', 'zip_code', 'datetime')\
                                    .dropDuplicates()

    # create dim_station
    station_df = st_station_df.select(F.col('id').alias('station_id'),
                                    F.col('name').alias('station_name'),
                                    'lat', 'long', 'dock_count', 'city',
                                    F.col('datetime').alias('installation_datetime') )

    station_df = station_df.join(st_city_df, station_df.city == st_city_df.city, 'left')\
                                .drop('city')\
                                .dropDuplicates()

    # make sure none of station or wheather data was dropped by mistake
    station_dim_count = station_df.count()
    weather_dim_count = weather_df.count()

    if station_dim_count != station_count or weather_dim_count != weather_count:
        raise Exception('Some dimensional rows are missing')
    else:
        print('All is good')

    # load (save) dim_staion to S3 in parquet fromat
    station_df.write.mode('overwrite')\
        .parquet('s3://omar-dend/dim_station')

    # load (save) dim_weather to S3 in parquet fromat partitioned by zip_code
    weather_df.write.mode('overwrite')\
        .partitionBy('zip_code')\
        .parquet('s3://omar-dend/dim_weather')

    # create dim_time
    time_df = st_station_df.select('datetime')\
        .withColumn('second', F.second('datetime'))\
        .withColumn('minute', F.minute('datetime'))\
        .withColumn('hour', F.hour('datetime'))\
        .withColumn('day', F.dayofmonth('datetime'))\
        .withColumn('week', F.weekofyear('datetime'))\
        .withColumn('month', F.month('datetime'))\
        .withColumn('year', F.year('datetime'))\
        .withColumn('weekday', F.dayofweek('datetime'))

    time_df = st_weather_df.select('datetime')\
        .withColumn('second', F.second('datetime'))\
        .withColumn('minute', F.minute('datetime'))\
        .withColumn('hour', F.hour('datetime'))\
        .withColumn('day', F.dayofmonth('datetime'))\
        .withColumn('week', F.weekofyear('datetime'))\
        .withColumn('month', F.month('datetime'))\
        .withColumn('year', F.year('datetime'))\
        .withColumn('weekday', F.dayofweek('datetime'))

    time_df = st_trip_df.select(F.col('datetime_start').alias('datetime'))\
        .withColumn('second', F.second('datetime'))\
        .withColumn('minute', F.minute('datetime'))\
        .withColumn('hour', F.hour('datetime'))\
        .withColumn('day', F.dayofmonth('datetime'))\
        .withColumn('week', F.weekofyear('datetime'))\
        .withColumn('month', F.month('datetime'))\
        .withColumn('year', F.year('datetime'))\
        .withColumn('weekday', F.dayofweek('datetime'))\

    time_df = st_trip_df.select(F.col('datetime_end').alias('datetime'))\
        .withColumn('second', F.second('datetime'))\
        .withColumn('minute', F.minute('datetime'))\
        .withColumn('hour', F.hour('datetime'))\
        .withColumn('day', F.dayofmonth('datetime'))\
        .withColumn('week', F.weekofyear('datetime'))\
        .withColumn('month', F.month('datetime'))\
        .withColumn('year', F.year('datetime'))\
        .withColumn('weekday', F.dayofweek('datetime'))\

    time_df = st_status_df.select('datetime')\
        .withColumn('second', F.second('datetime'))\
        .withColumn('minute', F.minute('datetime'))\
        .withColumn('hour', F.hour('datetime'))\
        .withColumn('day', F.dayofmonth('datetime'))\
        .withColumn('week', F.weekofyear('datetime'))\
        .withColumn('month', F.month('datetime'))\
        .withColumn('year', F.year('datetime'))\
        .withColumn('weekday', F.dayofweek('datetime'))\
        .dropDuplicates()

    # load (save) dim_weather to S3 in parquet fromat partitioned by year & month
    time_df.write.mode('overwrite')\
        .partitionBy('year', 'month')\
        .parquet('s3://omar-dend/dim_time')

    # create fact_trip
    trip_df = st_trip_df.select(F.col('id').alias('trip_id'), 'duration', 'bike_id',
                            'subscription_type',
                            'start_station_id', 'end_station_id',
                            'datetime_start', 'datetime_end')\
                            .dropDuplicates()

    # load (save) dim_weather to S3 in parquet fromat
    trip_df.write.mode('overwrite')\
        .parquet('s3://omar-dend/fact_trip')

    # create fact_status
    status_df = st_status_df.select('station_id', 'bikes_available',
                                'docks_available', 'datetime')\
                                .dropDuplicates()

    # load (save) dim_weather to S3 in parquet fromat partitioned by station_id
    status_df.write.mode('overwrite')\
        .partitionBy('station_id')\
        .parquet('s3://omar-dend/fact_status')

def main():
    spark = create_spark_session()
    process_data(spark)
    spark.stop()
