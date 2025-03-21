#!/bin/bash

# Parse options
OUTPUT_FILE=""
PROFILE="br-se1" # Default profile
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Missing value for option '$1'."
                echo "Usage: $0 [-o OUTPUT_FILE] [-p PROFILE] <Test_Category> <Path_Config> <Root_Folder>"
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
if [ "$#" -ne 3 ]; then
    echo "Error: Missing required arguments."
    echo "Usage: $0 [-o OUTPUT_FILE] [-p PROFILE] <Test_Category> <Path_Config> <Root_Folder>"
    exit 1
fi

# Assign variables
TEST_CATEGORY=$1
CONFIG_PATH=$2
ROOT_FOLDER=$3

# Validate test category
declare -A TEST_CATEGORIES=(
    ["full"]="acl_test.py big_objects_test.py cold_storage_test.py list-buckets_test.py locking_test.py locking_cli_test.py multiple_objects_test.py policies_test.py presigned-urls_test.py profiles_policies_test.py unique-bucket-name_test.py versioning_cli_test.py versioning_test.py"
    ["versioning"]="versioning_cli_test.py versioning_test.py"
    ["basic"]="acl_test.py list-buckets_test.py presigned-urls_test.py unique-bucket-name_test.py"
    ["policy"]="policies_test.py profiles_policies_test.py"
    ["cold"]="cold_storage_test.py"
    ["locking"]="locking_test.py locking_cli_test.py"
    ["big-objects"]="big_objects_test.py multiple_objects_test.py"
    ["consistency"]="consistency_test.py"
    ["benchmark"]="benchmark_test.py"
)

if [[ -z "${TEST_CATEGORIES[$TEST_CATEGORY]}" ]]; then
    echo "Invalid test category: $TEST_CATEGORY"
    echo "Valid categories: ${!TEST_CATEGORIES[@]}"
    exit 1
fi

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

# Define tests
TESTS=()
for test_file in ${TEST_CATEGORIES[$TEST_CATEGORY]}; do
    if [ -f "$ROOT_FOLDER/$test_file" ]; then
        TESTS+=("$ROOT_FOLDER/$test_file")
    else
        echo "Warning: Test file not found: $ROOT_FOLDER/$test_file"
    fi
done

if [ ${#TESTS[@]} -eq 0 ]; then
    echo "Error: No valid test files found for category: $TEST_CATEGORY"
    exit 1
fi

# Run pytest
echo "Running pytest..."
uv run pytest "${TESTS[@]}" --config "$CONFIG_PATH" -l -n auto -vv --tb=line --durations=0 | tee "$OUTPUT_FILE"

if [ $? -ne 0 ]; then
    echo "Pytest execution failed. Check the log file: $OUTPUT_FILE"
    exit 1
fi

# Generate report
uv run ./src/__main__.py --file_path "$OUTPUT_FILE"

# Clean logs
rm -f "$OUTPUT_FILE"

echo "Script execution completed."
