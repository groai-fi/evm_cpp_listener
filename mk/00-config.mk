# ==============================================================================
# Global Configuration & Environment Detection
# ==============================================================================

# Single source of truth for versioning
SEMVER     := $(strip $(shell cat VERSION 2>/dev/null || echo "0.1.0"))
GIT_DESC   := $(strip $(shell git describe --tags --always --dirty 2>/dev/null || echo "v$(SEMVER)"))
VERSION    := $(SEMVER)

# Build parallelism
BUILD_JOBS ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Container & Workspace paths
COMPOSE_FILE   := .devcontainer/docker-compose.yml
COMPOSE        := docker compose -f $(COMPOSE_FILE)
SERVICE_DEV    := dev
WORKSPACE_DIR  := /workspaces/evm_cpp_listener

# Detect if executing inside a container or on the host
ifeq ($(wildcard /.dockerenv),)
DEV_EXEC       := $(COMPOSE) exec -w $(WORKSPACE_DIR) -u vscode $(SERVICE_DEV)
ROOT_EXEC      := $(COMPOSE) exec -w $(WORKSPACE_DIR) -u root $(SERVICE_DEV)
IS_CONTAINER   := 0
else
DEV_EXEC       :=
ROOT_EXEC      :=
IS_CONTAINER   := 1
endif

# Terminal color tokens
BLUE   := \033[36m
GREEN  := \033[32m
YELLOW := \033[33m
RED    := \033[31m
RESET  := \033[0m
