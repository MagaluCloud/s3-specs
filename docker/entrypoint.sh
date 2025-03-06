#!/bin/bash
set -e

#configure profiles
uv run ./bin/configure_profiles.py params.example.yaml

uv sync

# Executar o comando passado para o contêiner
exec "$@"
