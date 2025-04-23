ARG TOOLS_IMAGE="ghcr.io/magalucloud/s3-specs:tools_nightly"

FROM ${TOOLS_IMAGE}

# Set the final working directory
WORKDIR /app

COPY . /app/
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