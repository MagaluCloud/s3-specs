# s3-specs-full
#
# This Container has:
#   - all 3 CLIs (rclone, aws, mgc)
#   - Python dependencies to run boto3 based tests.
ARG JUST_VERSION="1.40.0"
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

# Clone the AWS CLI v2 Dev repository
RUN git clone https://github.com/aws/aws-cli.git /usr/src/aws-cli && \
    cd /usr/src/aws-cli && \
    git checkout v2

RUN python3 -m venv /usr/src/aws-cli/venv && \
    . /usr/src/aws-cli/venv/bin/activate && \
    pip install -r /usr/src/aws-cli/requirements-dev.txt && \
    pip install -e /usr/src/aws-cli

ENV PATH="/usr/src/aws-cli/venv/bin:$PATH"

# Install Rclone Beta
RUN curl -Lo rclone.zip "https://beta.rclone.org/rclone-beta-latest-linux-amd64.zip" && \
    unzip -q rclone.zip && rm rclone.zip && \
    mv rclone-*-*-linux-amd64 /tools/rclone && \
    ln -s "/tools/rclone/rclone" /usr/local/bin/rclone



# Install MGC CLI DEV

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