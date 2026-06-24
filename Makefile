SHELL := /bin/bash
.ONESHELL:

.PHONY: install lint format check typecheck test clean dev db-upgrade db-migrate docker-build docker-compose-up
.PHONY: minikube-start minikube-stop deploy undeploy seed

# ── Python ─────────────────────────────────────────────────────────────────

install:
	uv sync --all-extras

lint:
	uv run ruff check src/ tests/

format:
	uv run black src/ tests/ alembic/
	uv run ruff check --fix src/ tests/

check: lint typecheck format
	@echo "All checks passed."

typecheck:
	uv run mypy src/

test:
	uv run pytest tests/ -v --cov=src/resource_broker --cov-report=term-missing

test-watch:
	uv run ptw tests/ -- -v --cov=src/resource_broker

clean:
	rm -rf .mypy_cache .pytest_cache *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ── Database ───────────────────────────────────────────────────────────────

db-migrate:
	uv run alembic upgrade head

db-downgrade:
	uv run alembic downgrade -1

db-revision:
	uv run alembic revision --autogenerate -m "$(message)"

# ── Docker ─────────────────────────────────────────────────────────────────

docker-build:
	docker build -t resource-broker-api:latest -f Dockerfile.broker-api .
	docker build -t resource-broker-performance-monitor:latest -f Dockerfile.performance-monitor .
	docker build -t resource-broker-recommender:latest -f Dockerfile.recommender .

docker-compose-up:
	docker compose up -d

docker-compose-down:
	docker compose down

# ── Minikube ───────────────────────────────────────────────────────────────

minikube-start:
	minikube start --cpus=4 --memory=8g --driver=docker
	minikube addons enable ingress
	minikube addons enable metrics-server

minikube-test:
	./scripts/setup-minikube.sh

minikube-stop:
	minikube stop

minikube-delete:
	minikube delete

# ── Deploy ─────────────────────────────────────────────────────────────────

deploy: deploy-ns deploy-broker

deploy-ns:
	kubectl apply -f deploy/namespace.yaml

deploy-broker:
	kubectl apply -f deploy/resource-broker/

undeploy:
	kubectl delete -f deploy/resource-broker/ 2>/dev/null || true
	kubectl delete -f deploy/namespace.yaml 2>/dev/null || true

seed:
	uv run python -m resource_broker seed --config scripts/seed-data.json

# ── Development (all-in-one) ───────────────────────────────────────────────

dev: docker-compose-up db-migrate seed
	@echo "Development environment ready."
	@echo "  API:       http://localhost:8080"
	@echo "  Docs:      http://localhost:8080/docs"
	@echo "  Prometheus: http://localhost:9090"
