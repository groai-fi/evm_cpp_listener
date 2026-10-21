# ==============================================================================
# Staging Environment Workflows
# ==============================================================================

.PHONY: staging-build
staging-build: ## Build project in RelWithDebInfo mode (optimized + debug symbols)
	@$(MAKE) build BUILD_TYPE=RelWithDebInfo

.PHONY: staging-test
staging-test: ## Run test suite against Staging build
	@$(MAKE) test BUILD_TYPE=RelWithDebInfo

.PHONY: staging-image
staging-image: ## Build staging Docker image
	@echo "$(GREEN)==> Building staging Docker image (tag: evm_listener:staging-$(VERSION))...$(RESET)"
	docker build -f docker/Dockerfile.prod --target runtime -t evm_listener:staging-$(VERSION) .
