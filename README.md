# AAPL-stock-market-pipeline

## Project Overview
This project is a stock market data pipeline that automates the process of fetching, transforming, and storing stock price data from the Yahoo Finance API. The pipeline is built using Apache Airflow, Apache Spark, and MinIO for storage. It supports loading transformed data into a PostgreSQL database and visualizing the results using Metabase.

## Workflow
The workflow consists of the following steps:

1. **Yahoo Finance API**: Fetches the stock prices.
2. **Airflow DAG**: Manages the tasks and workflow.
3. **MinIO**: Stores raw and formatted stock price data.
4. **Apache Spark**: Formats and processes the stock prices.
5. **PostgreSQL**: Stores the processed data.
6. **Metabase**: Visualizes the data from PostgreSQL.
7. **Slack Notifications**: Alerts users about pipeline status.

## Features
- **API Availability Check**: Ensures the Yahoo Finance API is available before fetching data.
- **Stock Data Fetching**: Pulls stock data from the Yahoo Finance API.
- **Storage in MinIO**: Raw data is stored in MinIO for intermediate processing.
- **Data Formatting**: Formats the data using Apache Spark.
- **CSV Export**: Exports formatted data back to MinIO in CSV format.
- **Data Warehouse Load**: Loads the processed data into a PostgreSQL data warehouse.
- **Slack Notifications**: Notifies of successful execution or any failures.
- **Metabase**: Allows for data visualization of stock trends.

  
