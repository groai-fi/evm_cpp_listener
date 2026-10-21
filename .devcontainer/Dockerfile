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
        # System Integration (libraries such as Boost/OpenSSL/simdjson come from Conan, not apt)
        uuid-dev \
        # Python tooling for Conan 2.x and helper test scripts
        python3 \
        python3-pip \
        python3-venv \
        # Redis CLI only; the Redis server runs as the separate `redis` compose service
        redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Install Conan 2.x globally
RUN pip3 install --no-cache-dir --break-system-packages 'conan==2.*'

# Workspace is a bind mount with a different owner uid; let git (used by `git describe` in CMake) trust it
RUN git config --system --add safe.directory /workspaces/evm_cpp_listener

# Pre-initialize Conan default profile for the non-root 'vscode' user
USER vscode
RUN conan profile detect --force && mkdir -p /home/vscode/.conan2/p
USER root

EXPOSE 8080 9464
