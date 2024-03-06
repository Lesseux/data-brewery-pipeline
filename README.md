# data-brewery-pipeline

Data pipeline consuming data from the API (<https://api.openbrewerydb.org/breweries>), transforming and persisting it into a data lake following the medallion architecture with three layers: raw data, curated data partitioned by location, and an analytical aggregated layer.

As is, the pipeline can be implemented in Google Cloud Plataform, using GCP Storage, Dataproc and Composer (V2) services (Airflow). 

Using the same GCP project for all the services, the steps for implementation:

1- Create a gcp storage bucket to host the dataproc cluster metadata and the following folders 
  i- data_lake_1: bronze layer
  ii- data_lake_2: silver layer
  iii- data_lake_3: gold layer
  iv- pyspark_operators: to host the pyspark ETL code (task1_3.py)

2- Create a Composer V2 enviroment and upload the dag code (dag_v3.py) to the dag folder (automatically generated in the enviroment).

3- The pipeline should run in about 10 minutes using the original dataproc cluster and compose enviroment configurations.  
