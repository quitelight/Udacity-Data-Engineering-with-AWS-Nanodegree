# Data Engineering with AWS Nanodegree
This repository is for the projects I've done on the [Udacity Data Engineering with AWS Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Project overview
### 1 - [Data Modeling with Apache Cassandra](https://github.com/knadoor/Udacity-Data-Engineering-with-AWS-Nanodegree/tree/main/1%20-%20Data%20Modeling%20with%20Apache%20Cassandra)
Development of an ETL pipeline to pull event data files from a remote source (such as S3), merging the data files as a single CSV and loading 
to a Apache Cassandra NoSQL database. The [following Stackoverflow page](https://stackoverflow.com/questions/41247345/python-read-cassandra-data-into-pandas) was beneficial in learning about how to read Cassandra data into a Pandas dataframe.
<br>
<b>Project overview:</b><br>
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis 
team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the 
results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring 
you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the 
analytics team from Sparkify to create the results.<br>
<b>Skills used:</b><br>
Cassandra Query Language (CQL), Python/Pandas, Jupyter

### 2 - [Data warehouse with AWS](https://github.com/knadoor/Udacity-Data-Engineering-with-AWS-Nanodegree/tree/main/2%20-%20Data%20warehouse%20with%20AWS)
For this project, a music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

To help Sparkify, I built an ETL pipeline that extracts their data from **S3**, stages them in **Redshift**, and transforms data into a set of **dimensional tables in Star Schema** for their analytics team to continue finding insights into what songs their users are listening to.

