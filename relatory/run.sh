#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 [-o OUTPUT_FILE] <Test_Category> <Path_Config> <Root_Folder>"
    echo "Options:"
    echo "  -o, --output OUTPUT_FILE  Specify the name of the output log file (optional)"
    echo "Test Categories: full, versioning, basic, policy, cold, locking, big-objects"
    exit 1
}

# Parse command-line options
OUTPUT_FILE=""  # Default output file name
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Missing value for option '$1'."
                usage
            fi
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --*)
            echo "Error: Invalid option '$1'."
            usage
            ;;
        -*)
            echo "Error: Invalid flag '$1'. Use '-o' or '--output' for the output file."
            usage
            ;;
        *)
            break
            ;;
    esac
done

# Check if the correct number of arguments is provided
if [ "$#" -ne 3 ]; then
    echo "Error: Missing required arguments."
    usage
fi

# Assign arguments to variables
TEST_CATEGORY=$1
CONFIG_PATH=$2
ROOT_FOLDER=$3

# Define test categories
declare -A TEST_CATEGORIES=(
    ["full"]="acl_test.py big_objects_test.py cold_storage_test.py list-buckets_test.py locking_test.py locking_cli_test.py multiple_objects_test.py policies_test.py presigned-urls_test.py profiles_policies_test.py unique-bucket-name_test.py versioning_cli_test.py versioning_test.py"
    ["versioning"]="versioning_cli_test.py versioning_test.py"
    ["basic"]="acl_test.py list-buckets_test.py presigned-urls_test.py unique-bucket-name_test.py"
    ["policy"]="policies_test.py profiles_policies_test.py"
    ["cold"]="cold_storage_test.py"
    ["locking"]="locking_test.py locking_cli_test.py"
    ["big-objects"]="big_objects_test.py multiple_objects_test.py"
)

# Validate test category
if [[ -z "${TEST_CATEGORIES[$TEST_CATEGORY]}" ]]; then
    echo "Invalid test category: $TEST_CATEGORY"
    echo "Valid categories: ${!TEST_CATEGORIES[@]}"
    exit 1
fi

# Validate config file
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Config file not found: $CONFIG_PATH"
    exit 1
fi

# Validate root folder
if [ ! -d "$ROOT_FOLDER" ]; then
    echo "Root folder not found: $ROOT_FOLDER"
    exit 1
fi

# Generate initialization time
INIT_TIME=$(date +"%Y%m%dT%H%M%S")

# Define log file name
if [ -z "$OUTPUT_FILE" ]; then
    OUTPUT_FILE="local-pytest-output.$INIT_TIME.log"
fi

# Define test paths based on category
TESTS=()
for test_file in ${TEST_CATEGORIES[$TEST_CATEGORY]}; do
    TESTS+=("$ROOT_FOLDER/$test_file")
done

# Execute pytest with UV
echo "Running pytest with UV..."
uv run pytest "${TESTS[@]}" --config $CONFIG_PATH -l -n auto -vv --tb=line --durations=0 | tee "$OUTPUT_FILE"

# Check if pytest execution was successful
if [ $? -eq 0 ]; then
    echo "Pytest execution completed successfully."
else
    echo "Pytest execution failed. Check the log file: $OUTPUT_FILE"
fi

# Generate report (add your report generation logic here)
echo "Generating report..."
uv run docs/__main__.py --file_path "$OUTPUT_FILE"

echo "Script execution completed."