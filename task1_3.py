import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import json


def create_spark_session():
    """
    Creates spark session

    :returns spark instance
    """
    spark = SparkSession.builder \
        .master("yarn") \
        .appName('task-1') \
        .getOrCreate()
    return spark


def get_data():
    """
    Retrieves data from the API: https://api.openbrewerydb.org/breweries

    Returns data (raw_data, str) and date_request (UTC, srt, format: YYYYMMDD_hhmmss)
    """

    response = requests.get("https://api.openbrewerydb.org/breweries")

    now = time.localtime()

    current_yyyymmdd = str(now.tm_year) + str(now.tm_mon).zfill(2) + str(now.tm_mday).zfill(2)

    current_hhmmss = str(now.tm_hour).zfill(2) + str(now.tm_min).zfill(2) + str(now.tm_sec).zfill(2)

    date_request = current_yyyymmdd + '_' + current_hhmmss

    data = response.text

    return data, date_request


def create_raw_data_layer(spark, data, date_request, path):
    """
    Creates a spark df with columns date_request(str: YYYYMMDD_hhmmss) and the raw API response (str)
    """
    line = [(date_request, data)]

    schema = StructType([
        StructField("data_request", StringType()),
        StructField("response", StringType())
    ])

    df = spark.createDataFrame(line, schema)

    full_path: str = f'{path}date_request={date_request}'
    df.write.mode('overwrite').parquet(path=full_path)


def create_tabular_layer(spark, data, date_request, path):
    """
    Reads raw data from data lake layer 1, transforms to tabular form.

    The resulting table is written in parquet format (partitioned by location: Country_State)
     into the directory given by the path/date_request=YYYYMMDD_hhmmss.
    """

    json_data = json.loads(data)

    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("brewery_type", StringType()),
        StructField("address_1", StringType()),
        StructField("address_2", StringType()),
        StructField("address_3", StringType()),
        StructField("city", StringType()),
        StructField("state_province", StringType()),
        StructField("postal_code", StringType()),
        StructField("country", StringType()),
        StructField("longitude", StringType()),
        StructField("latitude", StringType()),
        StructField("phone", StringType()),
        StructField("website_url", StringType()),
        StructField("state", StringType()),
        StructField("street", StringType())
    ])

    df = spark.createDataFrame(json_data, schema)

    df.registerTempTable('df_intermediary')

    df_data_lake_2 = spark.sql(f"""
        SELECT
            '{date_request}' AS date_request,
            id,
            name,
            brewery_type,
            address_1,
            address_2,
            address_3,
            city,
            state_province,
            postal_code,
            country,
            longitude,
            latitude,
            phone,
            website_url,
            state,
            street,
            CONCAT(COALESCE(country,'NotDefinedCountry'),'-',COALESCE(state,'NotDefinedCountry')) AS location
        FROM df_intermediary
    """)

    full_path: str = f'{path}date_request={date_request}'
    df_data_lake_2.write.mode('overwrite').partitionBy('location').parquet(path=full_path)

    return df_data_lake_2


def create_analytical_layer(spark, df_last_request, date_request, path):
    """
    Reads tabular data from data lake layer 2, transforms to aggregated form.

    The resulting table has total of breweries aggregated by location distinguishing different types.

    The resulting table is written in parquet format into the directory given by the path/date_request=YYYYMMDD_hhmmss.
    """

    df_last_request.registerTempTable('df_last_request')

    analytics_table_p1 = spark.sql("""
        SELECT 
            date_request,
            location,
            CASE 
                WHEN brewery_type = 'brewpub' THEN 1 ELSE 0 
            END AS brewpub,
            CASE 
                WHEN brewery_type = 'proprietor' THEN 1 ELSE 0
            END AS proprietor,
            CASE 
                WHEN brewery_type = 'contract' THEN 1 ELSE 0
            END AS contract,
            CASE 
                WHEN brewery_type = 'closed' THEN 1 ELSE 0
            END AS closed,
                    CASE 
                WHEN brewery_type = 'micro' THEN 1 ELSE 0 
            END AS micro,
            CASE 
                WHEN brewery_type = 'large' THEN 1 ELSE 0
            END AS large,
            CASE 
                WHEN brewery_type NOT IN ('brewpub','proprietor','contract','closed','micro','large') THEN 1 ELSE 0
            END AS other
        FROM df_last_request
    """)

    analytics_table_p1.registerTempTable('analytical_table_p1')

    analytics_table_p2 = spark.sql("""
        SELECT
            date_request,
            location,
            sum(brewpub) AS tot_brewpub,
            sum(proprietor) AS tot_proprietor,
            sum(contract) AS tot_contract,
            sum(closed) AS tot_closed,
            sum(micro) AS tot_micro,
            sum(large) AS tot_large,
            sum(other) AS tot_other,
            COUNT(0) AS tot_brewery
        FROM analytical_table_p1
        GROUP BY 
            date_request,
            location
    """)

    analytics_table_p2.registerTempTable('analytical_table_p2')

    analytics_table_p3 = spark.sql("""
        SELECT
            date_request,
            location,
            CAST(tot_brewpub AS INT)          AS    tot_brewpub,
            CAST(tot_proprietor AS INT)       AS    tot_proprietor,
            CAST(tot_contract AS INT)         AS    tot_contract,
            CAST(tot_closed AS INT)           AS    tot_closed,
            CAST(tot_micro AS INT)            AS    tot_micro,
            CAST(tot_large AS INT)            AS    tot_large,
            CAST(tot_other AS INT)            AS    tot_other,
            CAST(tot_brewery AS INT)          AS    tot_brewery
        FROM analytical_table_p2
    """)

    full_path: str = f'{path}tab_location_type_brewery/date_request={date_request}'
    analytics_table_p3.write.mode('overwrite').parquet(path=full_path)


def main():

    # creates a spark session
    spark = create_spark_session()

    # bronze
    path1 = 'gs://data-pipeline-brewery/data_lake_1/'
    # silver
    path2 = 'gs://data-pipeline-brewery/data_lake_2/'
    # gold
    path3 = 'gs://data-pipeline-brewery/data_lake_3/'

    # gets data at date_request (UTC) from the API
    data, date_request = get_data()

    # persists raw data into bronze layer
    create_raw_data_layer(spark, data, date_request, path1)

    # transform original data to tabular form partitioned by location (country_state) and loads into silver layer
    df_data_lake_2 = create_tabular_layer(spark, data, date_request, path2)
    df_data_lake_2.persist()

    # transform silver layer data to aggregated by location according to different types and loads into gold layer
    create_analytical_layer(spark, df_data_lake_2, date_request, path3)
    df_data_lake_2.unpersist()


if __name__ == "__main__":
    main()
