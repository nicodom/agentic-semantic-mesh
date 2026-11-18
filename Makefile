.PHONY: install lint fmt test smoke up down clean backfill generate peek jobs jobs-stop jobs-status demo eval

TOPIC ?= shop.payment_attempted

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

clean:  ## Stop the local stack and delete its data volumes (and the savepoints recorded in them)
	docker compose down -v
	rm -f data/local/flink/savepoints.json

backfill:  ## Generate the last 2 hours of events into Kafka, with a payment outage halfway
	uv run asm-generate backfill --hours 2 --seed 42 --scenario payment-outage

generate:  ## Generate live events until Ctrl-C, with a payment outage after 5 minutes
	uv run asm-generate live --scenario payment-outage

peek:  ## Show the latest events of a topic, decoded: make peek TOPIC=shop.order_placed
	uv run asm-peek $(TOPIC) --tail -n 2

jobs:  ## Start the Flink jobs, each from its last savepoint (ADR 0023)
	uv run asm-flink start

jobs-stop:  ## Stop the Flink jobs, each with a savepoint
	uv run asm-flink stop

jobs-status:  ## Show which Flink jobs are running
	uv run asm-flink status

demo:  ## Run the end-to-end demo
	@echo "not implemented yet (M1 step 9)"; exit 1

eval:  ## Run the agent eval suite
	@echo "not implemented yet (M1 step 8)"; exit 1
