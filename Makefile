.PHONY: install lint fmt test smoke up down clean demo eval

install:  ## Install Python and dependencies into .venv
	uv sync

lint:  ## Lint and check formatting
	uv run ruff check
	uv run ruff format --check

fmt:  ## Fix lint issues and format
	uv run ruff check --fix
	uv run ruff format

test:  ## Unit tests (no LLM calls, no running stack)
	uv run pytest -m "not eval and not integration"

smoke:  ## Smoke tests against the running stack (after `make up`)
	uv run pytest -m integration -v

up:  ## Start the local stack and wait until every service is healthy
	docker compose up -d --build --wait

down:  ## Stop the local stack (data is kept)
	docker compose down

clean:  ## Stop the local stack and delete its data volumes
	docker compose down -v

demo:  ## Run the end-to-end demo
	@echo "not implemented yet (M1 step 9)"; exit 1

eval:  ## Run the agent eval suite
	@echo "not implemented yet (M1 step 8)"; exit 1
