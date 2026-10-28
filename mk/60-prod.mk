# ==============================================================================
# Production Environment Workflows (Railway & Minimal Runtime)
# ==============================================================================

PROD_IMAGE_TAG ?= evm_listener:$(VERSION)

.PHONY: prod-build
prod-build: ## Build project in stripped Release mode (-O3, no sanitizers)
	@$(MAKE) build BUILD_TYPE=Release

.PHONY: prod-image
prod-image: ## Build minimal production Docker image (~80MB Debian slim)
	@echo "$(GREEN)==> Building production Docker image ($(PROD_IMAGE_TAG), evm-listener:prod)...$(RESET)"
	docker build -f docker/Dockerfile.prod -t $(PROD_IMAGE_TAG) -t evm-listener:prod --build-arg VERSION=$(GIT_DESC) .

.PHONY: prod-audit
prod-audit: prod-image ## Audit production container (binary version and shared libraries)
	@echo "$(GREEN)==> Auditing production binary version...$(RESET)"
	docker run --rm $(PROD_IMAGE_TAG) --version
	@echo "$(GREEN)==> Auditing shared library dependencies (ldd)...$(RESET)"
	docker run --rm --entrypoint ldd $(PROD_IMAGE_TAG) /usr/local/bin/evm_listener

.PHONY: prod-deploy
prod-deploy: ## Deploy to Railway via railway CLI
	@echo "$(GREEN)==> Deploying to Railway...$(RESET)"
	railway up
