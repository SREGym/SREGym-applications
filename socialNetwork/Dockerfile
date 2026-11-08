# Portions Copyright (c) Microsoft Corporation
# Licensed under the Apache License, Version 2.0. See LICENSE file in the project root for full license information.

ARG SOCIAL_NETWORK_BASE_IMAGE=ghcr.io/sregym/social-network-deps:sha-d2c036bc3d1138f5a0bdaffd3d87f37522c3461a@sha256:7d66d10f9bd05ae8b635e0a861dbb9dd02ffd9ef1a49e6c383c3d97a981fb119
FROM ${SOCIAL_NETWORK_BASE_IMAGE} AS builder
COPY docker/patch-redis-plus-plus.py /tmp/patch-redis-plus-plus.py

ARG LIB_REDIS_PLUS_PLUS_VERSION=1.2.3
ARG CMAKE_VERSION=3.27.9

# Remove the existing CMake
RUN apt-get remove --purge --auto-remove -y cmake

# Install required build tools. Kitware no longer publishes Xenial ARM64 packages,
# so CMake is built from its architecture-independent source release below.
RUN apt-get update && apt-get install -y \
    software-properties-common \
    lsb-release \
    apt-transport-https \
    wget \
    gnupg \
    libcurl4-gnutls-dev \
    build-essential \
    autoconf \
    libtool \
    pkg-config \
    libsystemd-dev \
    libc-ares-dev 

RUN wget -q https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.tar.gz \
    && tar -xzf cmake-${CMAKE_VERSION}.tar.gz \
    && cd cmake-${CMAKE_VERSION} \
    && ./bootstrap --prefix=/usr/local --parallel=$(nproc) \
    && make -j$(nproc) \
    && make install \
    && cmake --version \
    && cd / \
    && rm -rf cmake-${CMAKE_VERSION} cmake-${CMAKE_VERSION}.tar.gz

RUN add-apt-repository ppa:ubuntu-toolchain-r/test \
    && apt-get update \
    && apt-get install -y gcc-7 g++-7

# Rebuild the shared patched dependency with the application's GCC 7 toolchain.
RUN cd /tmp/redis-plus-plus\
&& python3 /tmp/patch-redis-plus-plus.py src/sw/redis++/redis_cluster.h \
&& cmake -DREDIS_PLUS_PLUS_USE_TLS=ON -DCMAKE_C_COMPILER=gcc-7 -DCMAKE_CXX_COMPILER=g++-7 . \
&& make -j$(nproc) \
&& make install

COPY ./ /social-network-microservices
RUN cd /social-network-microservices \
    && mkdir -p build \
    && cd build \
    # && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_C_COMPILER=gcc-7 -DCMAKE_CXX_COMPILER=g++-7 .. \
    && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc-7 -DCMAKE_CXX_COMPILER=g++-7 .. \
    && make -j`nproc` \
    && make install

FROM ubuntu:18.04

# Copy compiled C++ binaries and dependencies
COPY --from=builder /usr/local/bin/* /usr/local/bin/
COPY --from=builder /usr/local/lib/* /usr/local/lib/

# Install system dependencies
# iproute2 is for a chaos tool
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        openssl \
        libssl1.0.0 \
        ca-certificates \
        libsasl2-2 \
        libmemcached11 \
        libmemcachedutil2 \
        iproute2 \
        libcurl3-gnutls \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /social-network-microservices
