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
    gpg \
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

# Download da chave de verificação
RUN gpg --yes --keyserver keyserver.ubuntu.com --recv-keys 0C59E21A5CB00594 &&  gpg --export --armor 0C59E21A5CB00594 |  gpg --dearmor -o /etc/apt/keyrings/magalu-archive-keyring.gpg

# Adiciona repositório APT na lista de repositórios
RUN echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/magalu-archive-keyring.gpg] https://packages.magalu.cloud/apt stable main" |  tee /etc/apt/sources.list.d/magalu.list

# Instala a MGC CLI
RUN apt update
RUN apt install mgccli


# uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    ln -s $HOME/.local/bin/uv /usr/local/bin/uv;

# just (task runner, justfile)
ARG JUST_VERSION
RUN curl -LsSf https://just.systems/install.sh | bash -s -- --tag ${JUST_VERSION} --to /usr/local/bin;
