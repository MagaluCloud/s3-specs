set shell := ["bash", "-cu"]

@default:
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
