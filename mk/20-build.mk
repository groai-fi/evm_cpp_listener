# ==============================================================================
# C++ Build & Compilation
# ==============================================================================

.PHONY: cmake-configure
cmake-configure: ## Configure CMake build using current preset ($(CMAKE_PRESET))
	@echo "$(GREEN)==> Configuring CMake with $(CMAKE_PRESET) preset...$(RESET)"
	$(DEV_EXEC) cmake --preset $(CMAKE_PRESET)

.PHONY: build
build: ## Compile C++ project using current preset ($(CMAKE_PRESET))
	@echo "$(GREEN)==> Compiling C++ project ($(CMAKE_PRESET), parallel: $(BUILD_JOBS))...$(RESET)"
	$(DEV_EXEC) cmake --build --preset $(CMAKE_PRESET) --parallel $(BUILD_JOBS)

.PHONY: rebuild
rebuild: ## Clean build folder, reinstall dependencies, configure, and recompile
	@echo "$(YELLOW)==> Cleaning and rebuilding C++ project ($(BUILD_TYPE))...$(RESET)"
	$(DEV_EXEC) bash -c "rm -rf build/$(BUILD_TYPE) && conan install . --build=missing -pr:a=$(CONAN_PROFILE) -s build_type=$(BUILD_TYPE) && cmake --preset $(CMAKE_PRESET) && cmake --build --preset $(CMAKE_PRESET) --parallel $(BUILD_JOBS)"

.PHONY: clean-build
clean-build: ## Remove the local build/ directory
	@echo "$(YELLOW)==> Removing build directory...$(RESET)"
	rm -rf build
