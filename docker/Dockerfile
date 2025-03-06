FROM ubuntu:latest

ARG AWS_CLI_VERSION="2.15.27"
ARG RCLONE_VERSION="1.66.0"
ARG MGC_VERSION="0.34.1"

RUN apt-get update && \
    apt-get install -y \
    curl \
    unzip \
    python3 \
    python3-pip \
    git

RUN mkdir -p /tools && \
    curl -Lo rclone.zip "https://downloads.rclone.org/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-linux-amd64.zip" && \
    unzip -q rclone.zip && rm rclone.zip && \
    mv rclone-v${RCLONE_VERSION}-linux-amd64 /tools/ && \
    ln -s "/tools/rclone-v${RCLONE_VERSION}-linux-amd64/rclone" /usr/local/bin/rclone


RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

ARG MGC_VERSION
RUN curl -Lo mgc.tar.gz "https://github.com/MagaluCloud/mgccli/releases/download/v${MGC_VERSION}/mgccli_${MGC_VERSION}_linux_amd64.tar.gz" && \
    tar xzvf mgc.tar.gz && rm mgc.tar.gz && \
    ln -s "/tools/mgc" /usr/local/bin/mgc


RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    ln -s $HOME/.local/bin/uv /usr/local/bin/uv

# Define o diretório de trabalho para a raiz do projeto
WORKDIR /app

# Copia todo o conteúdo da pasta raiz do repositório para dentro do contêiner
COPY ../ /app


# Tornar o script executável
RUN chmod +x ./docker/entrypoint.sh

# Definir o script como ponto de entrada
ENTRYPOINT ["./docker/entrypoint.sh"]