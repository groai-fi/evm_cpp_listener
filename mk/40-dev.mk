# ==============================================================================
# Devcontainer & Local Stack Lifecycle
# ==============================================================================

.PHONY: up
up: ## Start the devcontainer stack (dev, redis, toxiproxy) in background
	@echo "$(GREEN)==> Starting dev containers...$(RESET)"
	$(COMPOSE) up -d

.PHONY: start
start: up ## Alias for 'up'

.PHONY: down
down: ## Stop the devcontainer stack
	@echo "$(YELLOW)==> Stopping dev containers...$(RESET)"
	$(COMPOSE) down

.PHONY: stop
stop: down ## Alias for 'down'

.PHONY: restart
restart: ## Restart all dev containers
	@echo "$(YELLOW)==> Restarting dev containers...$(RESET)"
	$(COMPOSE) restart

.PHONY: ps
ps: ## List status and ports of running dev containers
	$(COMPOSE) ps

.PHONY: status
status: ps ## Alias for 'ps'

.PHONY: logs
logs: ## Follow logs from all dev containers
	$(COMPOSE) logs -f

.PHONY: shell
shell: ## Open interactive zsh shell inside dev container (vscode user)
	$(DEV_EXEC) zsh

.PHONY: root-shell
root-shell: ## Open interactive bash shell as root
	$(ROOT_EXEC) bash

.PHONY: build-image
build-image: ## Build or update the devcontainer Docker image
	@echo "$(GREEN)==> Building dev container image...$(RESET)"
	$(COMPOSE) build

.PHONY: rebuild-image
rebuild-image: ## Clean rebuild of dev container image (no cache) and recreate
	@echo "$(YELLOW)==> Rebuilding dev container image from scratch (no cache)...$(RESET)"
	$(COMPOSE) build --no-cache
	$(COMPOSE) up -d --force-recreate

.PHONY: clean-all
clean-all: ## Stop containers, remove persistent volumes (Redis data), remove orphans
	@echo "$(YELLOW)==> Tearing down containers and removing persistent volumes...$(RESET)"
	$(COMPOSE) down -v --remove-orphans
