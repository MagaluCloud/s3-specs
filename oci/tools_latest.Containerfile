# s3-specs-full
#
# This Container have:
#   - all 3 CLIs (rclone, aws, mgc)
#   - Python dependencies to run boto3 based tests.

ARG AWS_CLI_VERSION="latest"
ARG JUST_VERSION="1.40.0"

# aws-cli
FROM public.ecr.aws/aws-cli/aws-cli:${AWS_CLI_VERSION} AS awscli

# Main image
FROM ubuntu:latest
RUN apt-get update && \
    apt-get install -y \
    curl \
    unzip \
    python3 \
    wget \
    git \
    fzf;

# directory to download binaries
RUN mkdir -p /tools;

# rclone
RUN curl -Lo rclone.zip "https://downloads.rclone.org/rclone-current-linux-amd64.zip" && \
    unzip -q rclone.zip && rm rclone.zip && \
    mv rclone-*-linux-amd64 /tools/rclone && \
    ln -s "/tools/rclone/rclone" /usr/local/bin/rclone
    
# aws cli
ARG AWS_CLI_VERSION
COPY --from=awscli /usr/local/aws-cli/ /tools/aws-cli/

# Cria os links usando o diretório real da versão copiada
RUN VERSION_DIR="$(find /tools/aws-cli/v2/ -maxdepth 1 -type d -name '[0-9]*.[0-9]*.[0-9]*' | head -n1)" && \
    ln -sf "${VERSION_DIR}/bin/aws" /usr/local/bin/aws && \
    ln -sf "${VERSION_DIR}/bin/aws_completer" /usr/local/bin/aws_completer
    
# mgc cli
RUN curl -fsSL https://raw.githubusercontent.com/marmotitude/mgc-installer/main/install.sh | bash
    # tar xzvf mgc.tar.gz && rm mgc.tar.gz && \
    # # ln -s "/tools/mgc" /usr/local/bin/mgc;
    # mv mgc /usr/local/bin/mgc

# uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    ln -s $HOME/.local/bin/uv /usr/local/bin/uv;

# just (task runner, justfile)
ARG JUST_VERSION
RUN curl -LsSf https://just.systems/install.sh | bash -s -- --tag ${JUST_VERSION} --to /usr/local/bin;

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

# Remove pinned boto3 from pyproject.toml
RUN uv remove boto3
# Add latest version of boto3
RUN uv add boto3

# Definir o script como ponto de entrada
ENTRYPOINT ["just"]
