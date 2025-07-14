ARG TOOLS_IMAGE="ghcr.io/magalucloud/s3-specs:tools_oldest"

FROM ${TOOLS_IMAGE}

# container workdir
WORKDIR /app

# Required files to execute the tests

COPY . /app/
# Download python dependencies to be bundled with the image
RUN uv sync

RUN bash -c "cd reports && uv sync"

# Definir o script como ponto de entrada
ENTRYPOINT ["just"]

