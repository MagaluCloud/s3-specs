ARG TOOLS_IMAGE="ghcr.io/magalucloud/s3-specs:tools_dev"

FROM ${TOOLS_IMAGE}

# container workdir
WORKDIR /app

# Required files to execute the tests
COPY bin /app/bin/
COPY params.example.yaml /app/params.example.yaml
COPY justfile /app/justfile
COPY utils.just /app/utils.just
COPY menu.just /app/menu.just
COPY uv.lock /app/uv.lock
COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md
COPY src/ /app/src/
COPY reports /app/reports/
COPY output /app/output/
# Download python dependencies to be bundled with the image
RUN uv sync

RUN bash -c "cd reports && uv sync"

# Definir o script como ponto de entrada
ENTRYPOINT ["just"]
