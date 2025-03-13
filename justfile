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
    uv run pytest ./docs/*_test.py --config {{config_file}} {{pytest_params}}

#Execute the tests of s3-specs
tests *pytest_params: setup-profiles
    just _run_tests "./params.example.yaml" {{pytest_params}}

#Execute the tests of s3-specs and generate a report of the tests after running.
report category:
    just setup-profiles
    bash -c "cd reports && ./run.sh {{category}} ../params.example.yaml ../docs/ && mv report_*.pdf outputs/"

# List known categories (pytest markers)
categories:
  just _extract_list "pyproject.toml" "tool.pytest.ini_options" "markers"

# Start a Jupyter lab server for browsing and executing the specs, right click and "Open as Notebook"
browse:
  uv run --with jupyter --with jupytext jupyter lab docs
