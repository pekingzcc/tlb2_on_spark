# tlb2_on_spark

## Overview

`tlb2_on_spark` is a project designed to process large-scale data by combining Apache Spark and TLB2.0. 
It enables TLB2.0 to run on Spark, facilitating efficient large-scale data processing.
Users can read large amounts of data from Moneta files stored in Google Cloud Storage,
aggregate and process this data using TLB2.0, and persist and reload Spark DataFrames.
The project leverages PySpark's capabilities to handle big data efficiently and
includes TLB2.0's stateful timeline calculation for persisting Spark DataFrames.

This allows users to utilize PySpark's DataFrame API for comprehensive data processing.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Modules](#modules)
    - [main.py](#mainpy)
    - [read_file.py](#read_filepy)
    - [persist.py](#persistpy)
- [Testing](#testing)

## Features

- **Data Reading**: Read larger amount of data from Moneta files stored in Google Cloud Storage.
- **Data Aggregation**: Aggregate data by partitions.
- **Data Processing**: Process aggregated data using TLB2.0.
- **Data Persistence**: Persist and reload Spark DataFrames.
- **Data Analyst**: Analyze data using PySpark's DataFrame API.

## Modules
main.py
Creates a Spark session, reads data, processes it, and stops the Spark session.  
read_file.py
Contains functions to read and process data from Avro files.  
persist.py
Provides a utility function to persist and reload Spark DataFrames.  
Testing
Unit tests are provided in the tests directory.