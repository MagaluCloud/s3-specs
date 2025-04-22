ARG TOOLS_IMAGE="ghcr.io/magalucloud/s3-specs:tools_dev"

FROM ${TOOLS_IMAGE}

# Set the final working directory
WORKDIR /app

# Copy the required files to execute the tests
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

# Download Python dependencies to be bundled with the image
RUN uv sync

RUN bash -c "cd reports && uv sync"

# Instale as dependências diretamente do Git

RUN uv remove boto3
RUN uv add git+https://github.com/boto/botocore.git@develop
RUN uv add git+https://github.com/boto/jmespath.git@develop
RUN uv add git+https://github.com/boto/s3transfer.git@develop

# Instale boto3 como editável
RUN uv add git+https://github.com/boto/boto3.git@develop

# Define the script as the entry point
ENTRYPOINT ["just"]
