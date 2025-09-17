FROM ubuntu:latest

WORKDIR /app/openhexa_templates

# Install required dependencies
RUN apt-get update && apt-get install -y apt-utils
RUN apt-get update && apt-get install -y \
    software-properties-common \
    build-essential \
    curl \
    wget \
    libssl-dev \
    libpq-dev \
    libkrb5-dev \
    libldap2-dev \
    libsasl2-dev \
    pkg-config \
    libmysqlclient-dev \
    zlib1g-dev \
    libffi-dev \
    libreadline-dev \
    libsqlite3-dev \
    libbz2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python 3.11 from source
RUN wget https://www.python.org/ftp/python/3.11.0/Python-3.11.0.tgz && \
    tar xvf Python-3.11.0.tgz && \
    cd Python-3.11.0 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && rm -rf Python-3.11.0 Python-3.11.0.tgz

# COPY ./requirements-dev.txt ./requirements-dev.txt
COPY ./requirements.txt ./requirements.txt

RUN python3.11 -m pip install --upgrade pip
RUN python3.11 -m pip install Cython==0.29.30
# RUN python3.11 -m pip install -r requirements-dev.txt
RUN python3.11 -m pip install -r requirements.txt

COPY ./ ./

