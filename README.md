# PROJECT: Data Pipelines

##About
In this project, building data pipelines with Apache Airflow. The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. To build data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.

## Requirement
  To start Airflow webserver, run /opt/airflow/start.sh
  Need to set up aws connection 
  Also set up redshift connection before running the dag
  
## Datasets
For this project, working with two datasets.

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

## Building Operators

   .StageToRedshiftOperator
   .LoadFactOperator
   .LoadDimensionOperator
   .DataQualityOperator
   
## DAG Result
   ![DAG graph](example-dag.png)
   