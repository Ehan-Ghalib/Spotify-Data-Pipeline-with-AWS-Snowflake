# AWS + Snowflake + Serverless: A Cloud-Native Near Real-Time Data Pipeline for Spotify API

## Project Overview

Designed and implemented a cloud-native pipeline to ingest, transform, and analyze Spotify playlist data for the Top 100 songs. This architecture leverages AWS services for automated scaling, alongside Snowflake for efficient data warehousing.

## Architecture Diagram

![Data Pipeline Architecture Diagram](https://github.com/Ehan-Ghalib/Spotify-Data-Pipeline-with-AWS-Snowflake/blob/53b8593231d3e6c135a393e3a84f5ba447fd06a9/Spotify%20AWS-Snowflake%20Pipeline%20Architecture%20Diagram.png)

### Data Ingestion

- A CloudWatch-triggered AWS Lambda function extracts Top 100 playlist data from the Spotify API.
- Raw JSON files are deposited in an S3 bucket for further processing.

### Data Transformation

- An S3 event triggers a second Lambda function that parses the raw JSON into structured CSV files for artists, albums, and tracks.
- These transformed files are stored in dedicated folders within S3.

### Data Loading into Snowflake

- An SQS notification alerts Snowflake whenever new files land in S3.
- Snowpipe automatically ingests the files into Snowflake tables, ensuring near-real-time availability of the latest data.

### Data Cataloging and Analysis

- AWS Glue Crawlers catalog the structured data, populating the Glue Data Catalog.
- Amazon Athena provides immediate querying of the S3 data.
- Snowflake hosts a consolidated view for deeper analysis and advanced reporting.

## Outcomes

- Serverless architecture and auto-ingestion pipelines minimize operational overhead.
- Data is readily available for analytics in both Athena and Snowflake.
- End-to-end automation reduces manual intervention, enabling agile data exploration.

This project showcases how AWS and Snowflake can work in tandem to build scalable, cost-effective data pipelines that deliver timely insights.

#Snowpipe, #Serverless, #Data Pipeline, #Near-Real Time
