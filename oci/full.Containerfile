# s3-specs-full
#
# This Container have:
#   - all 3 CLIs (rclone, aws, mgc)
#   - Python dependencies to run boto3 based tests.

ARG AWS_CLI_VERSION="2.15.27"
ARG RCLONE_VERSION="1.66.0"
ARG MGC_VERSION="0.34.1"
ARG JUST_VERSION="1.40.0"

# aws-cli
FROM public.ecr.aws/aws-cli/aws-cli:${AWS_CLI_VERSION} as awscli

# Main image
FROM ubuntu:latest
RUN apt-get update && \
    apt-get install -y \
    curl \
    unzip \
    python3 \
    git \
    fzf;

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
ARG MGC_VERSION
RUN curl -Lo mgc.tar.gz "https://github.com/MagaluCloud/mgccli/releases/download/v${MGC_VERSION}/mgccli_${MGC_VERSION}_linux_amd64.tar.gz" && \
    tar xzvf mgc.tar.gz && rm mgc.tar.gz && \
    ln -s "/tools/mgc" /usr/local/bin/mgc;

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
COPY docs /app/docs/
COPY reports /app/reports/
# Download python dependencies to be bundled with the image
RUN uv sync

RUN bash -c "cd reports && uv sync"

# Definir o script como ponto de entrada
ENTRYPOINT ["just"]
