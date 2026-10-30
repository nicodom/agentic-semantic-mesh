.PHONY: install lint fmt test up down demo eval

install:  ## Install Python and dependencies into .venv
	uv sync

lint:  ## Lint and check formatting
	uv run ruff check
	uv run ruff format --check

fmt:  ## Fix lint issues and format
	uv run ruff check --fix
	uv run ruff format

test:  ## Unit tests (no LLM calls)
	uv run pytest -m "not eval"

up:  ## Start the local stack
	@test -f compose.yaml || { echo "compose.yaml not added yet (M1 step 2)"; exit 1; }
	docker compose up -d

down:  ## Stop the local stack
	@test -f compose.yaml || { echo "compose.yaml not added yet (M1 step 2)"; exit 1; }
	docker compose down

demo:  ## Run the end-to-end demo
	@echo "not implemented yet (M1 step 9)"; exit 1

eval:  ## Run the agent eval suite
	@echo "not implemented yet (M1 step 8)"; exit 1
