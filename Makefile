.DEFAULT_GOAL := help

COMPOSE_FILE   := .devcontainer/docker-compose.yml
COMPOSE        := docker compose -f $(COMPOSE_FILE)
SERVICE_DEV    := dev
WORKSPACE_DIR  := /workspaces/evm_cpp_listener
DEV_EXEC       := $(COMPOSE) exec -w $(WORKSPACE_DIR) -u vscode $(SERVICE_DEV)
ROOT_EXEC      := $(COMPOSE) exec -w $(WORKSPACE_DIR) -u root $(SERVICE_DEV)
CONAN_PROFILE  := .devcontainer/conan/profiles/linux-gcc12
BUILD_TYPE     ?= Debug
BUILD_JOBS     ?= 4

# Colors for pretty terminal help
BLUE   := \033[36m
GREEN  := \033[32m
YELLOW := \033[33m
RESET  := \033[0m

# ==============================================================================
# Container Lifecycle Commands
# ==============================================================================

.PHONY: up
up: ## Start the devcontainer stack (dev, redis, toxiproxy) in background
	@echo "$(GREEN)==> Starting containers...$(RESET)"
	$(COMPOSE) up -d

.PHONY: start
start: up ## Alias for 'up'

.PHONY: down
down: ## Stop the devcontainer stack
	@echo "$(YELLOW)==> Stopping containers...$(RESET)"
	$(COMPOSE) down

.PHONY: stop
stop: down ## Alias for 'down'

.PHONY: restart
restart: ## Restart all containers
	@echo "$(YELLOW)==> Restarting containers...$(RESET)"
	$(COMPOSE) restart

.PHONY: ps
ps: ## List status and ports of running containers
	$(COMPOSE) ps

.PHONY: status
status: ps ## Alias for 'ps'

.PHONY: logs
logs: ## Follow logs from all containers
	$(COMPOSE) logs -f

.PHONY: shell
shell: ## Open an interactive zsh shell inside the dev container (as vscode user)
	$(DEV_EXEC) zsh

.PHONY: root-shell
root-shell: ## Open an interactive bash shell as root (for apt installs/debugging)
	$(ROOT_EXEC) bash

# ==============================================================================
# Container Image Build & Rebuild
# ==============================================================================

.PHONY: build-image
build-image: ## Build or update the devcontainer Docker image
	@echo "$(GREEN)==> Building dev container image...$(RESET)"
	$(COMPOSE) build

.PHONY: rebuild-image
rebuild-image: ## Clean rebuild of dev container image (no cache) and recreate containers
	@echo "$(YELLOW)==> Rebuilding dev container image from scratch (no cache)...$(RESET)"
	$(COMPOSE) build --no-cache
	$(COMPOSE) up -d --force-recreate

.PHONY: clean-all
clean-all: ## Stop containers, remove persistent volumes (Redis data), and remove orphans
	@echo "$(YELLOW)==> Tearing down containers and removing persistent volumes...$(RESET)"
	$(COMPOSE) down -v --remove-orphans

# ==============================================================================
# C++ Compilation & Test Commands (Executed inside dev container)
# ==============================================================================

.PHONY: conan
conan: ## Install C++ third-party dependencies via Conan 2.x inside container
	@echo "$(GREEN)==> Running Conan 2.x dependency installation...$(RESET)"
	$(DEV_EXEC) conan install . --build=missing -pr:a=$(CONAN_PROFILE) -s build_type=$(BUILD_TYPE)

.PHONY: cmake-configure
cmake-configure: ## Configure CMake build using Conan preset
	@echo "$(GREEN)==> Configuring CMake with conan-debug preset...$(RESET)"
	$(DEV_EXEC) cmake --preset conan-debug

.PHONY: build
build: ## Compile C++ project using conan-debug preset
	@echo "$(GREEN)==> Compiling C++ project...$(RESET)"
	$(DEV_EXEC) cmake --build --preset conan-debug --parallel $(BUILD_JOBS)

.PHONY: rebuild
rebuild: ## Clean build folder, reinstall dependencies, configure, and recompile
	@echo "$(YELLOW)==> Cleaning and rebuilding C++ project from scratch...$(RESET)"
	$(DEV_EXEC) bash -c "rm -rf build/Debug && conan install . --build=missing -pr:a=$(CONAN_PROFILE) -s build_type=$(BUILD_TYPE) && cmake --preset conan-debug && cmake --build --preset conan-debug --parallel $(BUILD_JOBS)"

.PHONY: test
test: ## Run all C++ unit and integration tests via CTest
	@echo "$(GREEN)==> Running test suite...$(RESET)"
	$(DEV_EXEC) ctest --preset conan-debug --output-on-failure

.PHONY: test-unit
test-unit: ## Run only pure unit tests (labeled 'unit')
	@echo "$(GREEN)==> Running unit tests...$(RESET)"
	$(DEV_EXEC) ctest --preset conan-debug -L unit --output-on-failure

.PHONY: test-integration
test-integration: ## Run only integration tests (Redis & Toxiproxy)
	@echo "$(GREEN)==> Running integration tests...$(RESET)"
	$(DEV_EXEC) ctest --preset conan-debug -L integration --output-on-failure

.PHONY: lint-lua
lint-lua: ## Lint Redis Lua scripts (luacheck w/ Redis sandbox globals, luac5.1 parse, #!lua/LF invariants)
	@echo "$(GREEN)==> Running luacheck on Lua scripts...$(RESET)"
	$(DEV_EXEC) tools/lint_lua.sh

.PHONY: clean-build
clean-build: ## Remove the local build/ directory
	@echo "$(YELLOW)==> Removing build directory...$(RESET)"
	rm -rf build

# ==============================================================================
# Help Menu
# ==============================================================================

.PHONY: help
help: ## Display this help message
	@echo ""
	@echo "$(BLUE)EVM C++ Listener - Devcontainer & Build Automation$(RESET)"
	@echo ""
	@echo "Usage: make $(GREEN)<target>$(RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-18s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
