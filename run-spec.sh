#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <spec_path> <yaml_params_path> <output_format>"
    exit 1
fi

NOTEBOOK_PATH=$1
YAML_PARAMS=$2
OUTPUT_FORMAT=$3
EXECUTION_NAME=$(basename "$YAML_PARAMS" .yaml)
PAPERMILL_OUTPUT_FOLDER="/tmp"
OUTPUT_FOLDER="docs/runs"

# Add docs to the PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)/docs

# Convert .py to .ipynb
TEMP_NOTEBOOK="/tmp/$(basename "$NOTEBOOK_PATH" .py).ipynb"
jupytext --to ipynb --output $TEMP_NOTEBOOK $NOTEBOOK_PATH
NOTEBOOK_PATH=$TEMP_NOTEBOOK

# Step 1: Run the notebook with Papermill using the YAML file for parameters
EXECUTED_NOTEBOOK="${PAPERMILL_OUTPUT_FOLDER}/$(basename "$NOTEBOOK_PATH" .ipynb)_${EXECUTION_NAME}.ipynb"
papermill $NOTEBOOK_PATH $EXECUTED_NOTEBOOK -y "config: $YAML_PARAMS" -y"docs_dir: docs" -k my-poetry-env

# Step 2: Convert the executed notebook to the specified format
jupyter nbconvert --to $OUTPUT_FORMAT $EXECUTED_NOTEBOOK --output-dir $OUTPUT_FOLDER

