FROM mcr.microsoft.com/devcontainers/cpp:1-bookworm

ENV PATH="/usr/local/bin:${PATH}"

# Install C++ toolchain, build tools, libraries, and runtime services for EVM Listener
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        pkg-config \
        gdb \
        lldb \
        clangd \
        clang-format \
        clang-tidy \
        valgrind \
        git \
        curl \
        ca-certificates \
        # Networking, Crypto & EVM Dependencies
        libssl-dev \
        libboost-all-dev \
        libsecp256k1-dev \
        # High-performance JSON parsing
        libsimdjson-dev \
        # Storage & System Integration
        libhiredis-dev \
        uuid-dev \
        libsystemd-dev \
        # Test frameworks
        catch2 \
        libgtest-dev \
        libgmock-dev \
        # Python tooling for Conan 2.x and helper test scripts
        python3 \
        python3-pip \
        python3-venv \
        # Local Redis server & CLI for local testing and stream verification
        redis-server \
        redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Install Conan 2.x globally
RUN pip3 install --no-cache-dir --break-system-packages conan

# Pre-initialize Conan default profile for the non-root 'vscode' user
USER vscode
RUN conan profile detect --force
USER root

EXPOSE 6379 8080
