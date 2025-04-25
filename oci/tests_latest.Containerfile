ARG TOOLS_IMAGE="ghcr.io/magalucloud/s3-specs:tools_latest"

FROM ${TOOLS_IMAGE}

# container workdir
WORKDIR /app

# Required files to execute the tests
COPY . /app/
# Download python dependencies to be bundled with the image
RUN uv sync

RUN bash -c "cd reports && uv sync"

# Remove pinned boto3 from pyproject.toml
RUN uv remove boto3
# Add latest version of boto3
RUN uv add boto3

# Definir o script como ponto de entrada
ENTRYPOINT ["just"]
