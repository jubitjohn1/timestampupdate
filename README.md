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
