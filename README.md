# Logistics-ETL

## Project Description 

This project implements an ETL (Extract, Transform, Load) pipeline tailored for logistics and freight data. It utilizes Apache Spark and Azure Data Lake Storage to ingest, clean, and transform data into a structured format suitable for analysis and reporting.

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Dependencies](#dependencies)
- [Configuration](#configuration)
- [Contributors](#contributors)
- [License](#license)

## Installation

Ensure the following tools/platforms are set up:

1. Apache Spark (e.g., via Databricks)
2. Azure Data Lake Storage (ADLS Gen2)
3. Python 3.x with Jupyter Notebook (for offline usage)
4. Required Spark libraries to support Delta Lake and Azure Blob integration

## Usage

The pipeline is divided into three main Jupyter notebooks:

### 1. Data Extraction

- Connects to Azure Blob Storage using SAS token
- Loads raw CSV data:
  - `Freight_Shipments.csv`
  - `Border_Crossing.csv`
  - `Port_Data.csv`
- Stores raw DataFrames for transformation

### 2. Data Transformation

- Converts raw data into a star schema using Delta Lake
- Creates:
  - Dimension Tables: `Dim_Ship`, `Dim_Bord`, `Dim_Potr`
  - Fact Tables: `Facts_Trade`, `Facts_Port_Borders`

### 3. Queries & Analysis

- Executes SQL queries on the transformed data to:
  - Rank ports by volume
  - Analyze trends by year and region
  - Assess performance of specific ports (e.g., Los Angeles, Houston)

## Features

- ETL pipeline using PySpark and SQL
- Integration with Azure cloud storage
- Structured star schema for analytics
- Sample queries for business intelligence insights

## Dependencies

- Apache Spark with Delta Lake support
- Azure Data Lake Storage (Gen2)
- Python 3.x
- Jupyter Notebooks

## Configuration

Ensure Spark is configured to authenticate with Azure Blob Storage using SAS:

```python
spark.conf.set("fs.azure.account.auth.type.<storage-account>", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>", "<SAS-TOKEN>")
