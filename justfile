# just file to store utility functions
import './utils.just'

# just module for TUI recipies
mod menu

# global variables
s3_tester_image := "ghcr.io/magalucloud/s3-tester:tests"

set shell := ["bash", "-cu"]

_default:
    @just --list

#Configure profiles in the CLI's
setup-profiles:
    uv run ./bin/configure_profiles.py ./params.example.yaml

# Run the tests
_run_tests config_file *pytest_params:
    uv run pytest ./src/s3_specs/docs/*_test.py --config {{config_file}} {{pytest_params}}

_run_tests_with_report category mark = "''":
    uv run python3 run_tests_and_generate_report.py {{category}} --mark {{mark}}


#Execute the tests of s3-specs
tests category mark = "" : setup-profiles
    # just _run_tests "./params.example.yaml" 
    just _run_tests_with_report {{category}} {{mark}}

#Execute the tests of s3-specs using pytest
tests-pytest *pytest_params: setup-profiles
    just _run_tests "./params.example.yaml" {{pytest_params}}

# List known categories (pytest markers)
categories:
  just _extract_list "pyproject.toml" "tool.pytest.ini_options" "markers"

# Start a Jupyter lab server for browsing and executing the specs, right click and "Open as Notebook"
browse:
  uv run --with jupyter --with jupytext jupyter lab src/s3_specs/docs

# Creates a page based on the execution of a spec and saves it under the docs/runs directory
_build-page spec_path config_file="./params/br-ne1.yaml" output_format="markdown":
  ./bin/run-spec.sh "{{spec_path}}" "{{config_file}}" "{{output_format}}"
