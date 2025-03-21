#!/bin/bash

# Parse options
OUTPUT_FILE=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Missing value for option '$1'."
                echo "Usage: $0 [-o OUTPUT_FILE] [<Test_Category>] <Path_Config> <Root_Folder> [<Bucket> [<Endpoint> [<Profile>]]]"
                exit 1
            fi
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -p|--profile)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Missing value for option '$1'."
                echo "Usage: $0 [-o OUTPUT_FILE] [-p PROFILE] <Test_Category> <Path_Config> <Root_Folder>"
                exit 1
            fi
            PROFILE="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

# Validate arguments
if [ "$#" -lt 2 ] || [ "$#" -gt 6 ]; then
    echo "Error: Incorrect number of arguments."
    echo "Usage: $0 [-o OUTPUT_FILE] [<Test_Category>] <Path_Config> <Root_Folder> [<Bucket> [<Endpoint> [<Profile>]]]"
    exit 1
fi

# Detect test category presence
num_args=$#
if [ $num_args -ge 3 ]; then
    TEST_CATEGORY=$1
    CONFIG_PATH=$2
    ROOT_FOLDER=$3
    shift 3
else
    TEST_CATEGORY=""
    CONFIG_PATH=$1
    ROOT_FOLDER=$2
    shift 2
fi

# Assign optional parameters
BUCKET=${1:-}
ENDPOINT=${2:-}
PROFILE=${3:-"br-se1"}

# Validate paths
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: File not found: $CONFIG_PATH"
    exit 1
fi

if [ ! -d "$ROOT_FOLDER" ]; then
    echo "Error: Directory not found: $ROOT_FOLDER"
    exit 1
fi

# Set log file name
INIT_TIME=$(date +"%Y%m%dT%H%M%S")
if [ -z "$OUTPUT_FILE" ]; then
    OUTPUT_FILE="local-pytest-output.$PROFILE.$INIT_TIME.log"
fi

# Build pytest command
PYTEST_CMD=(uv run pytest "$ROOT_FOLDER" --config "$CONFIG_PATH" -l -n auto -vv --tb=line --durations=0)
if [ -n "$TEST_CATEGORY" ]; then
    PYTEST_CMD+=(-m "$TEST_CATEGORY")
fi

# Run pytest
echo "Running pytest..."
"${PYTEST_CMD[@]}" | tee "$OUTPUT_FILE"

if [ $? -ne 0 ]; then
    echo "Pytest execution failed. Check the log file: $OUTPUT_FILE"
    exit 1
fi

# Resto do script mantido igual...
# [Seções de download, relatório, upload permanecem inalteradas]

# Download data
if [ -n "$BUCKET" ] && [ -n "$ENDPOINT" ]; then
    echo "Downloading data..."
    uv run ./src/generatedDataDownloader.py --config "$PROFILE" --endpoint "$ENDPOINT" --bucket "$BUCKET"
else
    echo "Skipping data download: Bucket or Endpoint not provided."
fi

# Generate report
uv run reports/src/__main__.py --file_path "$OUTPUT_FILE"

# Clean logs
rm -f *pytest*.log

# Upload artifacts
if [ -n "$BUCKET" ] && [ -n "$ENDPOINT" ]; then
    echo "Uploading artifacts..."
    uv run ./src/generatedDataUploader.py --profile "$PROFILE" --endpoint "$ENDPOINT" --bucket "$BUCKET"
else
    echo "Skipping data upload: Bucket or Endpoint not provided."
fi

echo "Script execution completed."