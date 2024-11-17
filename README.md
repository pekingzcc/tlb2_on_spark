# tlb2_on_spark

## Overview

`tlb2_on_spark` is a Python project designed for processing large-scale data using Apache Spark. 
It leverages the power of PySpark to handle big data efficiently and includes tlb2.0 stateful timeline to persist Spark DataFrames.
Then we can leverage the power of PySpark's dataframe API to process the data.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Modules](#modules)
    - [main.py](#mainpy)
    - [read_file.py](#read_filepy)
    - [persist.py](#persistpy)
- [Testing](#testing)

## Features

- **Data Reading**: Read data from Avro files stored in Google Cloud Storage.
- **Data Aggregation**: Aggregate data by partitions.
- **Data Processing**: Process aggregated data using Translation Lookaside Buffer (TLB).
- **Data Persistence**: Persist and reload Spark DataFrames.
- **Unit Testing**: Comprehensive unit tests for all major functionalities.

## Project Structure

tlb2_on_spark/
├── __init__.py
├── main.py
├── persist.py
├── read_file.py
tests/
├── __init__.py
├── test_main.py
.gitignore
README.md
requirements.txt
setup.py

## Modules
main.py
Creates a Spark session, reads data, processes it, and stops the Spark session.  
read_file.py
Contains functions to read and process data from Avro files.  
persist.py
Provides a utility function to persist and reload Spark DataFrames.  
Testing
Unit tests are provided in the tests directory.