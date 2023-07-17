# 3 - STEDI Human Balance Analytics

## Introduction

Spark and AWS Glue allow you to process data from multiple sources, categorize the data, and curate it to be queried in the future for multiple purposes. As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model. 

## Project Details

In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Project Details

As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Datasets
STEDI has three JSON data sources to use from the Step Trainer:
- customer
- step_trainer
- accelerometer

## Implementation

### Landing Zone

**Glue Tables**:
- customer_landing.sql
- accelerometer_landing.sql

### Trusted Zone

**Glue Job Scripts**:
- customer_landing_to_trusted.py
- accelerometer_landing_to_trusted.py
- step_trainer_landing_to_trusted.py
  
![image](https://github.com/quitelight/Udacity-Data-Engineering-with-AWS-Nanodegree/assets/139787492/26134dda-8fe4-488d-9a85-7089750d0a28)
![image](https://github.com/quitelight/Udacity-Data-Engineering-with-AWS-Nanodegree/assets/139787492/47447877-1730-4aa7-b919-1e105afbd1c0)
![image](https://github.com/quitelight/Udacity-Data-Engineering-with-AWS-Nanodegree/assets/139787492/f6d6cc22-08fd-4e22-9e71-10f44c60ef0d)

### Curated Zone

**Glue Job Scripts**:
- customer_trusted_to_curated.py
- machine_learning_curated.py
  
![image](https://github.com/quitelight/Udacity-Data-Engineering-with-AWS-Nanodegree/assets/139787492/8d53312a-61a3-47d0-a693-6b3f3235c0c6)
![image](https://github.com/quitelight/Udacity-Data-Engineering-with-AWS-Nanodegree/assets/139787492/09cd142c-c499-4073-8c07-b22996b617b6)

