#!/bin/bash

cd "$(dirname "$0")"

conda activate yelp_env || source /opt/anaconda3/etc/profile.d/conda.sh && conda activate yelp_env

set -a
source .env
set +a

export AIRFLOW_HOME="$(pwd)/src/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

echo "AIRFLOW_HOME=$AIRFLOW_HOME"
echo "DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"

echo
echo "Start these in separate terminals:"
echo "  airflow api-server"
echo "  airflow scheduler"
echo "  airflow dag-processor"
echo
echo "Then in a fourth terminal:"
echo "  airflow dags unpause yelp_processing_pipeline_final"
echo "  airflow dags trigger yelp_processing_pipeline_final"