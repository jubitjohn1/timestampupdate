# Device Status Spark Job

## Overview

This Spark job is designed to analyze and provide the latest update status (either "success" or "fail") of a device before a specified timestamp. It processes a dataset of device status updates and identifies the most recent status for each device at the given timestamp.

## Features

- Retrieves the latest device status ("success" or "fail") at a specified timestamp.
- Efficiently processes large datasets using Apache Spark.
- Provides flexibility to customize the timestamp for analysis.

## Prerequisites

- Apache Spark: Install Apache Spark on your cluster or local environment.
- Input Data: Prepare your device status data in a format compatible with the job's requirements.

## Usage

1. Clone the repository:

   ```bash
   git clone https://github.com/jubitjohn1/timestampupdate.git
   cd timestampupdate

## Demo CSV

```csv
cei_code,device_name,user_name,cei_status,updated_at
121,"A","abc","success",2023-08-12-07:01:55
122,"B","bcd","failed",2023-08-12-02:11:52
121,"A",null,"failed",2023-08-13-10:01:27
121,"C",null,"success",2023-08-13-12:01:55
121,"A","abc","failed",2023-08-12-02:11:52
```


## Config file

```json
   [

    {
        "cei_code":"121",
        "PK":"device_name"
    },
    {
        "cei_code":"122",
        "PK":"user_name"
    }

   ]
```
   
