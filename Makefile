# Fenced developer task runner.
#
# The targets mirror the CI gates in .github/workflows/ci.yml so a local
# check and a pipeline check answer the same question. Run `make help` for
# the list; AGENTS.md explains which gate to trust for which change.
#
# Override the toolchain when it is not on PATH:
#   make GO=/opt/go/bin/go PYTHON=python3.12 verify

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

GO ?= go
PYTHON ?= python3
NPM ?= npm
COMPOSE ?= docker compose

DEV_COMPOSE := deploy/dev/compose.yaml
DATABASE_URL ?= postgres://fenced:fenced-dev-only@127.0.0.1:55432/fenced?sslmode=disable
NATS_URL ?= nats://127.0.0.1:54222
PRODUCT_VERSION ?= 1.4.0.0
RUST_TOOLCHAIN ?= 1.97.1
PG_IMAGE := pgvector/pgvector:pg18@sha256:2ba9ca5f2e7daa0f0e7723cba1ee9167bab54efd3640516a44ac1a928dd67e7a
TAKEOVER_CONTAINER := fenced-readme-postgres
TAKEOVER_PORT ?= 55434
TAKEOVER_URL := postgres://fenced:fenced-readme-only@127.0.0.1:$(TAKEOVER_PORT)/fenced_readme?sslmode=disable

.DEFAULT_GOAL := help
.PHONY: help build fmt fmt-check vet lint vuln test test-race test-integration \
	python-test python-wheel ts-test web-build web-test web-verify docs-verify capability-verify capability-verify-strict proto rust-build rust-lint rust-test \
	version-check conformance dev-up dev-down dev-migrate dev-reset \
	takeover-check verify verify-ci clean

##@ General

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nFenced task runner\n\nUsage: make <target>\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)
	@echo ""

##@ Build

build: ## Build every control-plane command
	$(GO) build ./cmd/...

##@ Static checks

fmt: ## Format the Go sources in place
	gofmt -w $(shell find . -name '*.go' -not -path './gen/*')

fmt-check: ## Fail when a Go source is not gofmt-clean
	@test -z "$$(gofmt -l . )" || (gofmt -l . ; echo "run: make fmt" ; exit 1)

vet: ## Run go vet
	$(GO) vet ./...

lint: ## Run staticcheck (same version as CI)
	$(GO) run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

vuln: ## Scan reachable Go vulnerabilities
	$(GO) tool govulncheck ./...

version-check: ## Assert the product version constant
	@test "$$($(GO) run ./cmd/fenced version -json | $(PYTHON) -c 'import json,sys; print(json.load(sys.stdin)["productVersion"])')" = "$(PRODUCT_VERSION)" \
		|| (echo "product version mismatch" ; exit 1)

##@ Tests

test: ## Run the Go unit tests
	$(GO) test -count=1 ./...

test-race: ## Run the Go unit tests with the race detector
	$(GO) test -race -count=1 ./...

test-integration: ## Run the integration suites (needs a PostgreSQL instance)
	@test -n "$(FENCED_TEST_DATABASE_URL)" || (echo "set FENCED_TEST_DATABASE_URL to a disposable PostgreSQL" ; exit 1)
	$(GO) test -race -tags=integration -count=1 -timeout 60m ./...

python-test: ## Run the Python SDK and adapter tests
	$(PYTHON) -m compileall -q sdk/python/fenced_runtime adapters examples/agents
	cd sdk/python && $(PYTHON) -m unittest discover -s tests

python-wheel: ## Build the Python SDK wheel
	$(PYTHON) -m pip wheel --no-deps --wheel-dir dist/wheels sdk/python

ts-test: ## Run the TypeScript SDK tests
	cd sdk/typescript && $(NPM) ci && $(NPM) test

web-build: ## Build the management console bundle
	cd web && $(NPM) ci && $(NPM) run build

web-test: ## Test the management console
	cd web && $(NPM) ci && $(NPM) test && $(NPM) run typecheck

web-verify: ## Verify the committed console bundle matches its sources
	cd web && $(NPM) ci && $(NPM) run build
	git diff --exit-code -- internal/control/console/dist

docs-verify: ## Check that every document is either published or local
	$(PYTHON) scripts/check-docs-policy.py

capability-verify: ## Run the proof of every capability the matrix claims
	$(PYTHON) scripts/check-capability-proofs.py

capability-verify-strict: ## The proof check CI runs: a tool that cannot run is a failure
	$(PYTHON) scripts/check-capability-proofs.py --strict

##@ Contracts

proto: ## Lint the protobuf contracts and verify the generated bindings
	@command -v buf >/dev/null || (echo "buf is required: go install github.com/bufbuild/buf/cmd/buf@v1.72.0" ; exit 1)
	buf lint
	buf generate
	git diff --exit-code -- gen/go

##@ Rust (Wasmtime provider)

rust-build: ## Build the Wasmtime provider and the conformance component
	cargo +$(RUST_TOOLCHAIN) build --bin fenced-runtime-wasm --locked
	cargo +$(RUST_TOOLCHAIN) run --bin component-fixture --locked -- --out target/conformance/agent.wasm

rust-lint: ## Check the Rust formatting and run clippy
	cargo +$(RUST_TOOLCHAIN) fmt --all -- --check
	cargo +$(RUST_TOOLCHAIN) clippy --workspace --all-targets --locked -- -D warnings

rust-test: ## Test the Wasmtime provider
	cargo +$(RUST_TOOLCHAIN) test --workspace --locked

##@ Runtime Interface conformance

conformance: ## Check a Runtime Interface adapter against the conformance suite
	PYTHONPATH=./sdk/python $(GO) run ./cmd/fenced-conformance \
		-cmd "$(PYTHON) examples/agents/python_remote/server.py --port 0" -timeout 30s

##@ Local control plane

dev-up: ## Start PostgreSQL and NATS, then apply the migrations
	$(COMPOSE) -f $(DEV_COMPOSE) up -d --wait postgres nats
	$(MAKE) dev-migrate

dev-down: ## Stop the local control plane and drop its volumes
	$(COMPOSE) -f $(DEV_COMPOSE) down -v

dev-reset: ## Recreate the local database from scratch
	$(COMPOSE) -f $(DEV_COMPOSE) down -v
	$(MAKE) dev-up

dev-migrate: ## Apply the database migrations to the local instance
	$(GO) run ./cmd/fenced-migrate -database-url '$(DATABASE_URL)'

##@ Evidence

takeover-check: ## Prove the fencing takeover with a disposable PostgreSQL
	@docker rm --force --volumes $(TAKEOVER_CONTAINER) >/dev/null 2>&1 || true
	@echo "waiting for port $(TAKEOVER_PORT) to become bindable"
	@for attempt in $$(seq 1 90); do \
		if $(PYTHON) -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',$(TAKEOVER_PORT))); s.close()" 2>/dev/null; then \
			break; \
		fi; \
		sleep 1; \
	done
	@docker run --detach --name $(TAKEOVER_CONTAINER) --publish 127.0.0.1:$(TAKEOVER_PORT):5432 \
		--env POSTGRES_USER=fenced --env POSTGRES_PASSWORD=fenced-readme-only \
		--env POSTGRES_DB=fenced_readme $(PG_IMAGE) >/dev/null
	@trap 'docker rm --force --volumes $(TAKEOVER_CONTAINER) >/dev/null 2>&1 || true' EXIT; \
		for attempt in $$(seq 1 30); do \
			docker exec $(TAKEOVER_CONTAINER) pg_isready -U fenced -d fenced_readme >/dev/null 2>&1 && break; \
			sleep 1; \
		done; \
		FENCED_TEST_DATABASE_URL='$(TAKEOVER_URL)' $(GO) test -tags=integration -count=1 \
			-run '^TestFencingReplayRejectedAfterTakeover$$' -v ./internal/security

##@ Gates

verify: ## Local gate: formatting, vet, unit tests, Python SDK, version
	$(MAKE) fmt-check
	$(MAKE) vet
	$(MAKE) test
	$(MAKE) python-test
	$(MAKE) version-check

verify-ci: ## Everything CI runs that is available on this machine
	$(MAKE) build
	$(MAKE) fmt-check
	$(MAKE) vet
	$(MAKE) lint
	$(MAKE) vuln
	$(MAKE) test-race
	$(MAKE) python-test
	$(MAKE) ts-test
	$(MAKE) web-test
	$(MAKE) web-verify
	$(MAKE) docs-verify
	$(MAKE) capability-verify-strict
	$(MAKE) version-check
	@command -v buf >/dev/null && $(MAKE) proto || echo "skip proto: buf is not installed"
	@command -v cargo >/dev/null && $(MAKE) rust-lint rust-test || echo "skip rust: cargo is not installed"

clean: ## Remove build and test artifacts
	rm -rf dist tmp/artifacts target
	$(GO) clean -testcache
