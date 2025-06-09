# s3-specs-full
#
# This Container have:
#   - all 3 CLIs (rclone, aws, mgc)
#   - Python dependencies to run boto3 based tests.

ARG AWS_CLI_VERSION="2.15.27"
ARG RCLONE_VERSION="1.66.0"
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
    git \
    fzf \
    gpg;

# directory to download binaries
RUN mkdir -p /tools;

# rclone
ARG RCLONE_VERSION
RUN curl -Lo rclone.zip "https://downloads.rclone.org/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-linux-amd64.zip" && \
    unzip -q rclone.zip && rm rclone.zip && \
    mv rclone-v${RCLONE_VERSION}-linux-amd64 /tools/ && \
    ln -s "/tools/rclone-v${RCLONE_VERSION}-linux-amd64/rclone" /usr/local/bin/rclone;

# aws cli
ARG AWS_CLI_VERSION
COPY --from=awscli /usr/local/aws-cli/ /tools/aws-cli/
RUN ln -s "/tools/aws-cli/v2/${AWS_CLI_VERSION}/bin/aws" /usr/local/bin/aws && \
    ln -s "/tools/aws-cli/v2/${AWS_CLI_VERSION}/bin/aws_completer" /usr/local/bin/aws_completer;

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
