# ==============================================================================
# Staging Environment Workflows (Railway Staging & Pre-Prod Verification)
# ==============================================================================

STAGING_IMAGE_TAG ?= evm_listener:staging-$(VERSION)

.PHONY: staging-build
staging-build: ## Build project in RelWithDebInfo mode (optimized + debug symbols)
	@$(MAKE) build BUILD_TYPE=RelWithDebInfo

.PHONY: staging-test
staging-test: ## Run test suite against Staging build
	@$(MAKE) test BUILD_TYPE=RelWithDebInfo

.PHONY: staging-image
staging-image: ## Build staging Docker image (~80MB Debian slim)
	@echo "$(GREEN)==> Building staging Docker image ($(STAGING_IMAGE_TAG), evm-listener:staging)...$(RESET)"
	docker build -f docker/Dockerfile.prod -t $(STAGING_IMAGE_TAG) -t evm-listener:staging --build-arg VERSION=$(GIT_DESC) .

.PHONY: staging-audit
staging-audit: staging-image ## Audit staging container (binary version and shared libraries)
	@echo "$(GREEN)==> Auditing staging binary version...$(RESET)"
	docker run --rm $(STAGING_IMAGE_TAG) --version
	@echo "$(GREEN)==> Auditing shared library dependencies (ldd)...$(RESET)"
	docker run --rm --entrypoint ldd $(STAGING_IMAGE_TAG) /usr/local/bin/evm_listener

.PHONY: staging-deploy
staging-deploy: ## Deploy to Railway staging environment via railway CLI
	@echo "$(GREEN)==> Deploying to Railway (staging)...$(RESET)"
	railway up -e staging
