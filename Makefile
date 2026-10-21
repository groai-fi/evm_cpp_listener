.DEFAULT_GOAL := help

# ==============================================================================
# Modular Make System
# ==============================================================================
# Components are organized under mk/ by concern:
#   00-config.mk   : Global paths, environment detection, and version metadata
#   10-conan.mk    : Conan 2.x dependency caching and profiles
#   20-build.mk    : CMake compilation parameterized by $(BUILD_TYPE)
#   30-test.mk     : CTest runner (unit, integration) and Lua script linter
#   40-dev.mk      : Devcontainer and local mock stack lifecycle
#   50-staging.mk  : Staging configuration and verification
#   60-prod.mk     : Minimal production container and deployment
#   70-version.mk  : Single-source semantic version inspection and bumping
# ==============================================================================

include mk/00-config.mk
include mk/10-conan.mk
include mk/20-build.mk
include mk/30-test.mk
include mk/40-dev.mk
include mk/50-staging.mk
include mk/60-prod.mk
include mk/70-version.mk

# ==============================================================================
# Interactive Help Menu
# ==============================================================================

.PHONY: help
help: ## Display this help message
	@echo ""
	@echo "$(BLUE)====================================================================$(RESET)"
	@echo "$(BLUE)  EVM C++ Listener - Build Automation & Ops (v$(SEMVER))$(RESET)"
	@echo "$(BLUE)====================================================================$(RESET)"
	@echo ""
	@echo "Usage: make $(GREEN)<target>$(RESET) [BUILD_TYPE=Debug|RelWithDebInfo|Release]"
	@echo ""
	@echo "$(YELLOW)Available Targets:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
