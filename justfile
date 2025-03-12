# just submodule to store utility functions
mod utils

set shell := ["bash", "-cu"]

_default:
    just --list

#Configure profiles in the CLI's
setup-profiles:
    uv run ./bin/configure_profiles.py ./params.example.yaml

#Execute the tests of s3-specs
tests *pytest_params:
    just setup-profiles
    uv run pytest ./docs/*_test.py --config ./params.example.yaml {{pytest_params}}

#Execute the tests of s3-specs and generate a report of the tests after running.
report category:
    just setup-profiles
    bash -c "cd reports && ./run.sh {{category}} ../params.example.yaml ../docs/ && mv report_*.pdf outputs/"

# List known categories (pytest markers)
categories:
  just utils extract_list "pyproject.toml" "tool.pytest.ini_options" "markers"

# List legacy categories (shellspec tags of legacy s3-tester)
_legacy-categories:
  just utils extract_list "pyproject.toml" "tool.s3-tester" "markers"

# Start a Jupyter lab server for browsing and executing the specs, right click and "Open as Notebook"
browse:
  uv run --with jupyter --with jupytext jupyter lab docs

