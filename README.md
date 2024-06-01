# End-to-End Automated Sensor Data Processing Pipeline Implementation
## _Case Study : Developing a Streaming Data Processing Pipeline using Dummy Sensor Data_

[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

[![GitHub watchers](https://img.shields.io/github/watchers/tmuchlissin/python-project-for-data-engineering.svg?style=social&label=Watch&maxAge=2592000)](https://GitHub.com/tmuchlissin/python-project-for-data-engineering/watchers/)


## Overview
This project showcases an automated data processing pipeline leveraging Apache Kafka, PySpark, MongoDB, and MySQL technologies with dummy sensor data for real-time data streaming, processing, storage, and visualization, demonstrating proficiency in data engineering and stream processing techniques. 

## Workflow
![image](https://github.com/tmuchlissin/automated-sensor-data-processing-pipeline-implementation/assets/117092055/64707e24-2c2d-4ed6-a022-60e1224704da)

- Data Generation :
    - Generated dummy sensor data with attributes including timestamp, ID, city, watt, volt, and ampere using Python code.
    - Sent the generated data to Kafka as a publisher for further processing.
- Data Consumption :
    - Developed a Kafka subscriber to consume data and integrated it with PySpark for real-time stream processing.
- Streaming Data Processing :
    - Implemented a streaming data pipeline to handle raw data and perform preprocessing and aggregation using PySpark.
    - Created windowing operations per minute, per hour, and per day to summarize and calculate averages for watt, volt, and ampere attributes.
- Data Storage :
    - Utilized MongoDB as a NoSQL database to store raw sensor data.
    - Implemented MySQL database for storing aggregated sensor data based on windowed intervals (per minute, hour, and day).
- Visualization and Monitoring :
    - Configured a Grafana dashboard to monitor and visualize the aggregated data stored in MySQL.



