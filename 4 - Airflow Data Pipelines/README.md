# 4 - Airflow Data Pipelines

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Airflow Data Pipeline DAG
The process for setting up the data pipeline involved:
1. Create IAM User in AWS
2. Configure Redshift Serverless in AWS
3. Start the Airflow Web Server by running `/opt/airflow/start-services.sh` followed by `/opt/airflow/start.sh`
4. Start the Airflow scheduler by running `airflow scheduler`
5. Create AWS and Redshift Serverless connections in Airflow Web Server UI
6. Airflow will pickup the DAG to run from the "dags" folder

![image](https://github.com/quitelight/Udacity-Data-Engineering-with-AWS-Nanodegree/assets/139787492/29212014-cef6-4989-a0e5-78abd72021d8)
