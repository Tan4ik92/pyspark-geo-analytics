# PySpark Geo Analytics

A batch analytics pipeline that processes geospatial user events, builds data marts and generates location-aware friend recommendations.

## Architecture

```text
Raw events in HDFS / Parquet
              ↓
       PySpark transformations
      ↙          ↓           ↘
User mart    Zone mart    Friend recommendations
              ↑
        Apache Airflow
```

## Data products

- Enriched messages with the nearest city and local time zone.
- User mart with current city, home city and travel history.
- Zone mart with event and user activity statistics.
- Friend recommendations for users who share subscriptions, have not messaged each other and are located within one kilometre.

## Engineering highlights

- Window functions for selecting the latest user location.
- Spatial distance calculations using the Haversine formula.
- Anti-joins to exclude existing conversations.
- Spark jobs executed on YARN and orchestrated by Apache Airflow.
- Parquet input/output and explicit Spark resource configuration.

## Technologies

Python, PySpark, Apache Spark, Apache Airflow, Hadoop/YARN, HDFS, Parquet.

## Repository structure

```text
dags/geo_datamarts.py
jobs/messages_with_city_timezone.py
jobs/users_datamart.py
jobs/zones_datamart.py
jobs/friends_recommendation.py
```

## Running the project

Update the HDFS input and output paths in the job configuration for your environment, copy the jobs to the Spark cluster and deploy the Airflow DAG. The DAG executes the user, zone and recommendation marts in dependency order.

This repository contains no credentials or private datasets.
