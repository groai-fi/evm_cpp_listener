# ==============================================================================
# C++ Test Suite & Lua Linting
# ==============================================================================

.PHONY: test
test: ## Run all C++ unit and integration tests via CTest
	@echo "$(GREEN)==> Running full test suite ($(CMAKE_PRESET))...$(RESET)"
	$(DEV_EXEC) ctest --preset $(CMAKE_PRESET) --output-on-failure

.PHONY: test-unit
test-unit: ## Run only pure unit tests (label 'unit')
	@echo "$(GREEN)==> Running unit tests ($(CMAKE_PRESET))...$(RESET)"
	$(DEV_EXEC) ctest --preset $(CMAKE_PRESET) -L unit --output-on-failure

.PHONY: test-integration
test-integration: ## Run only integration tests (label 'integration')
	@echo "$(GREEN)==> Running integration tests ($(CMAKE_PRESET))...$(RESET)"
	$(DEV_EXEC) ctest --preset $(CMAKE_PRESET) -L integration --output-on-failure

.PHONY: lint-lua
lint-lua: ## Lint Redis Lua scripts (luacheck + luac5.1 + invariants)
	@echo "$(GREEN)==> Running luacheck on Lua scripts...$(RESET)"
	$(DEV_EXEC) tools/lint_lua.sh
