# s3-specs-full
#
# This Container has:
#   - all 3 CLIs (rclone, aws, mgc)
#   - Python dependencies to run boto3 based tests.
ARG JUST_VERSION="1.40.0"
ARG AWS_CLI_VERSION="2.15.27"
ARG RCLONE_VERSION="1.66.0"
# Main image
FROM ubuntu:latest
RUN apt-get update && \
    apt-get install -y \
    curl \
    unzip \
    python3 \
    python3-pip \
    python3-venv \
    wget \
    git \
    fzf

RUN wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz && \
    ln -s "/usr/local/go/bin/go" /usr/local/bin/go
# Create a directory to download binaries
RUN mkdir -p /tools

ENV PATH="/usr/local/bin:${PATH}"

# aws cli
ARG AWS_CLI_VERSION
COPY --from=awscli /usr/local/aws-cli/ /tools/aws-cli/
RUN ln -s "/tools/aws-cli/v2/${AWS_CLI_VERSION}/bin/aws" /usr/local/bin/aws && \
    ln -s "/tools/aws-cli/v2/${AWS_CLI_VERSION}/bin/aws_completer" /usr/local/bin/aws_completer;


ENV PATH="/usr/src/aws-cli/venv/bin:$PATH"

# rclone
ARG RCLONE_VERSION
RUN curl -Lo rclone.zip "https://downloads.rclone.org/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-linux-amd64.zip" && \
    unzip -q rclone.zip && rm rclone.zip && \
    mv rclone-v${RCLONE_VERSION}-linux-amd64 /tools/ && \
    ln -s "/tools/rclone-v${RCLONE_VERSION}-linux-amd64/rclone" /usr/local/bin/rclone;



# Install MGC CLI DEV on branch main
RUN git clone https://github.com/MagaluCloud/magalu.git mgc-cli && \
    cd mgc-cli/mgc/cli/ && \
    go install && \
    go build -o mgc && \
    ln -s "/mgc-cli/mgc/cli/mgc" /usr/local/bin/mgc

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    ln -s $HOME/.local/bin/uv /usr/local/bin/uv

# Install just (task runner, justfile)
ARG JUST_VERSION
RUN curl -LsSf https://just.systems/install.sh | bash -s -- --tag ${JUST_VERSION} --to /usr/local/bin

RUN git clone https://github.com/boto/boto3.git boto3

