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

.PHONY: lint-cpp
lint-cpp: ## Run clang-tidy on C++ sources
	@echo "$(GREEN)==> Running clang-tidy on C++ sources...$(RESET)"
	$(DEV_EXEC) bash -c "find evm_listener/src evm_listener/tests -name '*.cpp' -print0 | xargs -0 -n 4 -P $(BUILD_JOBS) clang-tidy -p build/$(BUILD_TYPE)"

.PHONY: format-check
format-check: ## Check C++ formatting with clang-format
	@echo "$(GREEN)==> Checking C++ formatting with clang-format...$(RESET)"
	$(DEV_EXEC) bash -c "find evm_listener/src evm_listener/tests \\( -name '*.hpp' -o -name '*.cpp' \\) -print0 | xargs -0 clang-format --dry-run --Werror"

.PHONY: test-tsan
test-tsan: ## Build and run C++ tests with ThreadSanitizer (TSan)
	@echo "$(GREEN)==> Building and running test suite with ThreadSanitizer (TSan)...$(RESET)"
	$(DEV_EXEC) bash -c "cmake --preset conan-debug -DEVML_TSAN=ON -DEVML_SANITIZE=OFF -DEVML_TEST_HOOKS=ON && cmake --build --preset conan-debug --parallel $(BUILD_JOBS) && ctest --preset conan-debug --output-on-failure && cmake --preset conan-debug -DEVML_TSAN=OFF -DEVML_SANITIZE=ON -DEVML_TEST_HOOKS=ON && cmake --build --preset conan-debug --parallel $(BUILD_JOBS)"

