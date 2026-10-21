# ==============================================================================
# Semantic Versioning & Metadata
# ==============================================================================

.PHONY: version
version: ## Display current SemVer (from VERSION) and Git describe string
	@echo "$(BLUE)EVM Listener Version Metadata:$(RESET)"
	@echo "  Semantic Version (VERSION) : $(GREEN)$(SEMVER)$(RESET)"
	@echo "  Git Describe (Full Tag)    : $(GREEN)$(GIT_DESC)$(RESET)"

.PHONY: bump-patch
bump-patch: ## Bump patch version in VERSION file (e.g. 0.1.0 -> 0.1.1)
	@awk -F. '{$$NF = $$NF + 1;} 1' OFS=. VERSION > VERSION.tmp && mv VERSION.tmp VERSION
	@echo "$(GREEN)==> Updated VERSION to $$(cat VERSION)$(RESET)"

.PHONY: bump-minor
bump-minor: ## Bump minor version in VERSION file (e.g. 0.1.0 -> 0.2.0)
	@awk -F. '{$$2 = $$2 + 1; $$3 = 0;} 1' OFS=. VERSION > VERSION.tmp && mv VERSION.tmp VERSION
	@echo "$(GREEN)==> Updated VERSION to $$(cat VERSION)$(RESET)"

.PHONY: bump-major
bump-major: ## Bump major version in VERSION file (e.g. 0.1.0 -> 1.0.0)
	@awk -F. '{$$1 = $$1 + 1; $$2 = 0; $$3 = 0;} 1' OFS=. VERSION > VERSION.tmp && mv VERSION.tmp VERSION
	@echo "$(GREEN)==> Updated VERSION to $$(cat VERSION)$(RESET)"
