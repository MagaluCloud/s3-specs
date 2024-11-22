#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <spec_path> <yaml_params_path> <output_format>"
    exit 1
fi

SPEC_PATH=$(realpath $1)
YAML_PARAMS=$(realpath $2)
OUTPUT_FORMAT=$3
SPEC_NAME=$(basename "$SPEC_PATH" .py)
EXECUTION_NAME=$(basename "$YAML_PARAMS" .yaml)
OUTPUT_FOLDER="docs/runs"

# Add docs to the PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)/docs
export SPEC_PATH=${SPEC_PATH}
export CONFIG=${YAML_PARAMS}

# Convert .py to .ipynb and execute it
EXECUTED_NOTEBOOK="/tmp/${SPEC_NAME}_${EXECUTION_NAME}.ipynb"
jupytext --to notebook --execute --output $EXECUTED_NOTEBOOK $SPEC_PATH --warn-only

# Parse the executed notebook for errors
if grep -qE '"ename":|Traceback|ERROR|E ' $EXECUTED_NOTEBOOK; then
    echo -e "\033[31mError: Notebook execution failed. See $EXECUTED_NOTEBOOK for details.\033[0m"
    exit 1
fi

# Convert the executed notebook to the specified format
jupyter nbconvert --to $OUTPUT_FORMAT $EXECUTED_NOTEBOOK --output-dir $OUTPUT_FOLDER

