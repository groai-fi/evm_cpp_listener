# ==============================================================================
# Conan 2.x Dependency Management
# ==============================================================================

BUILD_TYPE ?= Debug

ifeq ($(BUILD_TYPE),Release)
  CONAN_PROFILE := conan/profiles/release
  CMAKE_PRESET  := conan-release
else ifeq ($(BUILD_TYPE),RelWithDebInfo)
  CONAN_PROFILE := conan/profiles/relwithdebinfo
  CMAKE_PRESET  := conan-relwithdebinfo
else
  CONAN_PROFILE := conan/profiles/debug
  CMAKE_PRESET  := conan-debug
endif

.PHONY: conan
conan: ## Install C++ dependencies via Conan 2.x inside container
	@echo "$(GREEN)==> Installing Conan dependencies (profile: $(CONAN_PROFILE), build_type: $(BUILD_TYPE))...$(RESET)"
	$(DEV_EXEC) conan install . --lockfile=conan.lock --build=missing -pr:a=$(CONAN_PROFILE) -s build_type=$(BUILD_TYPE)
