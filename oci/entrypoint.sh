#!/bin/bash
set -e

#configure profiles
uv run ./bin/configure_profiles.py params.yaml

# Executar o comando passado para o contêiner
cd docs
exec uv run pytest *_test.py --config ../params.example.yaml "$@"
