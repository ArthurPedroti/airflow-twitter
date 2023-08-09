#!/bin/bash

# Export AIRFLOW_HOME variable
export AIRFLOW_HOME=$(pwd)/airflow

# Activate the virtual environment
source /path/to/your/virtualenv/bin/activate

# Start Airflow
airflow standalone
