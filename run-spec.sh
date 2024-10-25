#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <notebook_path> <yaml_params_path> <output_format>"
    exit 1
fi

NOTEBOOK_PATH=$1
YAML_PARAMS=$2
OUTPUT_FORMAT=$3
EXECUTION_NAME=$(basename "$YAML_PARAMS" .yaml)
PAPERMILL_OUTPUT_FOLDER="/tmp"
OUTPUT_FOLDER="docs"

# Step 1: Run the notebook with Papermill using the YAML file for parameters
EXECUTED_NOTEBOOK="${PAPERMILL_OUTPUT_FOLDER}/$(basename "$NOTEBOOK_PATH" .ipynb)_${EXECUTION_NAME}.ipynb"
papermill $NOTEBOOK_PATH $EXECUTED_NOTEBOOK -y "$(cat $YAML_PARAMS)"

# Step 2: Convert the executed notebook to the specified format
jupyter nbconvert --to $OUTPUT_FORMAT $EXECUTED_NOTEBOOK --output-dir $OUTPUT_FOLDER

