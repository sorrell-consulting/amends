GO ?= go
MKDOCS ?= mkdocs
PYTHON ?= python3
GO_VERSION := $(shell sed -n 's/^go //p' go.mod)
export GOTOOLCHAIN := go$(GO_VERSION)
export GOWORK := off
export AMENDS_ARTIFACT_DIR := $(CURDIR)/artifacts/failures
export AMENDS_TEST_DATABASE_URL ?= postgres://amends:amends-local-only@127.0.0.1:15432/amends?sslmode=disable
export AMENDS_TEST_BROKERS ?= 127.0.0.1:19092
SEED ?= 1
MODE ?= all
DRILL ?= crash-mid-correction
export AMENDS_DATABASE_URL ?= $(AMENDS_TEST_DATABASE_URL)
BROKERS ?= $(AMENDS_TEST_BROKERS)

COMPOSE := docker compose --project-name amends-local --file docker-compose.yml

.DEFAULT_GOAL := check
.PHONY: test-admin evidence-admin
.PHONY: docs-build docs-check docs-serve
.PHONY: help fmt fmt-check vet test test-postgres evidence-postgres test-worker evidence-worker test-delivery evidence-delivery test-verify evidence-verify test-showcase evidence-showcase build check core-demo demo drill sim sim-delivery test-sim-delivery sim-worker test-sim-worker test-mutations evidence-mutations evidence deps-config up down status ready

help:
	@printf '%s\n' \
	  'Start here:' \
	  '  make core-demo  Run the correction example in memory (no services)' \
	  '  make demo MODE=correction-only Run the durable correction' \
	  '  make drill DRILL=crash-mid-correction Run the owned worker SIGKILL drill' \
	  '  make demo       Run both durable correction and worker-crash stories' \
	  '  make help       Show this task-oriented command list' \
	  '' \
	  'Local dependencies (Docker Compose):' \
	  '  make deps-config Validate the local dependency configuration' \
	  '  make up         Start local dependencies and wait for health checks' \
	  '  make ready      Probe the running database and broker (does not start them)' \
	  '  make status     Show dependency state, including stopped containers' \
	  '  make down       Stop local dependencies; preserve containers and volumes' \
	  '' \
	  'Contributor checks (make defaults to check):' \
	  '  make check      Check formatting, vet, test with the race detector, and build' \
	  '  make fmt        Format Go packages' \
	  '  make fmt-check  Check formatting without changing source files' \
	  '  make vet        Run go vet' \
	  '  make test       Run race-enabled tests without cached results (no services)' \
	  '  make build      Build bin/amendsctl and bin/amends' \
	  '' \
	  'Documentation (install requirements-docs.txt first):' \
	  '  make docs-serve Preview the Material for MkDocs site locally' \
	  '  make docs-build Build site/ with strict warnings' \
	  '  make docs-check Strict build, links, and fixture/chart consistency' \
	  '' \
	  'Integration tests and advanced simulations:' \
	  '  make test-postgres Run real database tests against the configured local endpoint' \
	  '  make test-worker Run source worker tests against PostgreSQL and Redpanda' \
	  '  make test-delivery Run real relay, materializer, and full pipeline tests' \
	  '  make test-verify Run bounded verification tests against PostgreSQL and Redpanda' \
	  '  make test-admin Run controlled replay and isolated rebuild integration tests' \
	  '  make test-showcase Run process lifecycle and real showcase integration tests' \
	  '  make sim SEED=N Run and save a seeded memory-model trace' \
	  '  make sim-delivery SEED=N Save and run production delivery schedules with memory I/O' \
	  '  make test-sim-delivery Run delivery schedules and the fixed 32-run corpus' \
	  '  make sim-worker SEED=N Save and run production worker schedules with memory I/O' \
	  '  make test-sim-worker Run worker lifecycle schedules and the fixed 16-run corpus' \
	  '  make test-mutations Run isolated guard mutations (requires local PostgreSQL); save evidence' \
	  '' \
	  'Advanced drills (details and expected statuses: docs/showcase.md):' \
	  '  make drill DRILL=replay-intact Check replay preserves intact state and versions' \
	  '  make drill DRILL=full-rebuild Check fresh reconstruction and namespace isolation' \
	  '  make drill DRILL=poison-event Check quarantine and exact diagnostic qualification' \
	  '  make drill DRILL=source-conflict Check blocked authority and qualified repair' \
	  '  make drill DRILL=relay-stall Check incomplete publication and recovery' \
	  '  make drill DRILL=disappearing-day Check withdrawal survives older redelivery' \
	  '  make drill DRILL=materializer-crash Kill the view before and after a durable withdrawal' \
	  '  make drill DRILL=duplicate-flood Check duplicates preserve business versions' \
	  '  make drill DRILL=stale-amendment-storm Check lower revisions cannot replace authority' \
	  '  make drill DRILL=history-missing Remove an owned source prefix; require safe refusals' \
	  '  make drill DRILL=zombie-worker Fence a stale worker; verify unchanged state and recovery' \
	  '  make drill DRILL=ambiguous-commit Lose real COMMIT requests/replies; verify durable recovery' \
	  '  make drill DRILL=rebalance-under-load Reassign two populated partitions with queued corrections' \
	  '' \
	  'Retained evidence:' \
	  '  make evidence   Save toolchain, corpora, tests, demo, and simulation artifacts' \
	  '  make evidence-postgres Save real database test results in artifacts/postgres/' \
	  '  make evidence-worker Save broker/worker test results in artifacts/worker/' \
	  '  make evidence-delivery Save transport test results in artifacts/delivery/' \
	  '  make evidence-verify Save verification test results in artifacts/verification/' \
	  '  make evidence-admin Save administrative integration evidence in artifacts/operations/' \
	  '  make evidence-showcase Save process-crash tests and retained run artifacts' \
	  '  make evidence-mutations Alias for the explicit mutation campaign'

docs-build:
	$(MKDOCS) build --strict

docs-check: docs-build
	$(PYTHON) -m unittest discover -s scripts -p test_check_docs.py
	$(PYTHON) scripts/check_docs.py

docs-serve:
	$(MKDOCS) serve

fmt:
	@set -eu; \
	  gofmt="$$($(GO) env GOROOT)/bin/gofmt"; \
	  find . -type d \( -name .git -o -name .local -o -name vendor \) -prune -o \
	    -type f -name '*.go' -exec "$$gofmt" -w {} +

fmt-check:
	@set -eu; \
	  gofmt="$$($(GO) env GOROOT)/bin/gofmt"; \
	  unformatted="$$(find . -type d \( -name .git -o -name .local -o -name vendor \) -prune -o \
	    -type f -name '*.go' -exec "$$gofmt" -l {} +)"; \
	  if [ -n "$$unformatted" ]; then \
	    printf 'Run make fmt for:\n%s\n' "$$unformatted"; \
	    exit 1; \
	  fi

vet:
	$(GO) vet -mod=readonly ./...

test:
	$(GO) test -mod=readonly -race -count=1 ./...

test-postgres:
	$(GO) test -mod=readonly -race -count=1 -tags=integration ./internal/store/postgres

test-worker:
	$(GO) test -mod=readonly -race -count=1 -tags=integration ./internal/worker

test-delivery:
	$(GO) test -mod=readonly -race -count=1 -tags=integration ./internal/delivery

test-verify:
	$(GO) test -mod=readonly -race -count=1 -tags=integration ./internal/delivery -run '^TestVerification'

test-admin:
	$(GO) test -mod=readonly -race -count=1 -tags=integration ./internal/admin

evidence-admin:
	mkdir -p artifacts/operations
	$(GO) version > artifacts/operations/toolchain.txt
	$(GO) test -mod=readonly -race -count=1 -tags=integration -json ./internal/admin > artifacts/operations/tests.json || { test_exit_code=$$?; cat artifacts/operations/tests.json; exit $$test_exit_code; }

evidence-verify:
	mkdir -p artifacts/verification
	$(GO) version > artifacts/verification/toolchain.txt
	$(GO) list -m all > artifacts/verification/modules.txt
	$(GO) test -mod=readonly -race -count=1 -tags=integration -json ./internal/delivery -run '^TestVerification' > artifacts/verification/tests.json || { test_exit_code=$$?; cat artifacts/verification/tests.json; exit $$test_exit_code; }

evidence-delivery:
	mkdir -p artifacts/delivery
	$(GO) version > artifacts/delivery/toolchain.txt
	$(GO) list -m all > artifacts/delivery/modules.txt
	$(GO) test -mod=readonly -race -count=1 -tags=integration -json ./internal/delivery > artifacts/delivery/tests.json || { test_exit_code=$$?; cat artifacts/delivery/tests.json; exit $$test_exit_code; }

evidence-worker:
	mkdir -p artifacts/worker
	$(GO) version > artifacts/worker/toolchain.txt
	$(GO) list -m all > artifacts/worker/modules.txt
	$(GO) test -mod=readonly -race -count=1 -tags=integration -json ./internal/worker > artifacts/worker/tests.json || { test_exit_code=$$?; cat artifacts/worker/tests.json; exit $$test_exit_code; }

evidence-postgres:
	mkdir -p artifacts/postgres
	$(GO) version > artifacts/postgres/toolchain.txt
	$(GO) list -m all > artifacts/postgres/modules.txt
	$(GO) test -mod=readonly -race -count=1 -tags=integration -json ./internal/store/postgres > artifacts/postgres/tests.json || { test_exit_code=$$?; cat artifacts/postgres/tests.json; exit $$test_exit_code; }

build:
	$(GO) build -mod=readonly -buildvcs=false -o bin/amendsctl ./cmd/amendsctl
	$(GO) build -mod=readonly -buildvcs=false -o bin/amends ./cmd/amends

check: fmt-check vet test build

core-demo: build
	./bin/amendsctl core-demo

demo: build up
	$(MAKE) ready
	./bin/amendsctl demo -mode '$(MODE)' -brokers '$(BROKERS)'

drill: build up
	$(MAKE) ready
	@case '$(DRILL)' in \
	  crash-mid-correction) ./bin/amendsctl demo -mode crash-mid-correction -brokers '$(BROKERS)' ;; \
	  replay-intact|full-rebuild|poison-event|source-conflict|relay-stall|disappearing-day|materializer-crash|duplicate-flood|stale-amendment-storm|history-missing|zombie-worker|ambiguous-commit|rebalance-under-load) ./bin/amendsctl drill -name '$(DRILL)' -brokers '$(BROKERS)' ;; \
	  *) printf '%s\n' 'Supported drills: crash-mid-correction, replay-intact, full-rebuild, poison-event, source-conflict, relay-stall, disappearing-day, materializer-crash, duplicate-flood, stale-amendment-storm, history-missing, zombie-worker, ambiguous-commit, rebalance-under-load'; exit 2 ;; \
	esac

test-showcase: build
	$(GO) test -mod=readonly -race -count=1 -tags=integration ./internal/showcase

evidence-showcase: build
	mkdir -p artifacts/showcase
	$(GO) version > artifacts/showcase/toolchain.txt
	$(GO) list -m all > artifacts/showcase/modules.txt
	$(COMPOSE) images > artifacts/showcase/images.txt
	@if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then \
	  git rev-parse HEAD > artifacts/showcase/revision.txt; \
	  git status --short > artifacts/showcase/worktree.txt; \
	else printf '%s\n' 'Source archive; Git metadata unavailable' > artifacts/showcase/revision.txt; fi
	AMENDS_SHOWCASE_ARTIFACT_DIR='$(CURDIR)/artifacts/showcase' $(GO) test -mod=readonly -race -count=1 -tags=integration -json ./internal/showcase > artifacts/showcase/tests.json || { test_exit_code=$$?; cat artifacts/showcase/tests.json; exit $$test_exit_code; }

sim: build
	./bin/amendsctl sim -seed '$(SEED)' -trace 'artifacts/sim-seed-$(SEED).json'

sim-delivery: build
	./bin/amendsctl sim-delivery -seed '$(SEED)' -trace 'artifacts/delivery-seed-$(SEED).json'

test-sim-delivery:
	$(GO) test -mod=readonly -race -count=1 ./sim/transport

sim-worker: build
	./bin/amendsctl sim-worker -seed '$(SEED)' -trace 'artifacts/worker-seed-$(SEED).json'

test-sim-worker:
	$(GO) test -mod=readonly -race -count=1 ./internal/worker ./sim/ownership

# Explicit opt-in: never a dependency of check, test, or ordinary evidence.
test-mutations evidence-mutations:
	$(GO) run -mod=readonly ./tools/mutations -out artifacts/mutations

evidence: fmt-check vet build
	mkdir -p artifacts/evidence
	$(GO) version > artifacts/evidence/toolchain.txt
	@if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then \
	  git rev-parse HEAD > artifacts/evidence/revision.txt; \
	  git status --short > artifacts/evidence/worktree.txt; \
	else \
	  printf '%s\n' 'Source archive; Git revision unavailable' > artifacts/evidence/revision.txt; \
	  printf '%s\n' 'Source archive; Git status unavailable' > artifacts/evidence/worktree.txt; \
	fi
	cp test/property/corpus.json artifacts/evidence/corpus.json
	cp sim/transport/testdata/corpus.json artifacts/evidence/delivery-corpus.json
	cp sim/ownership/testdata/corpus.json artifacts/evidence/worker-corpus.json
	$(GO) test -mod=readonly -race -count=1 -json ./... > artifacts/evidence/tests.json || { test_exit_code=$$?; cat artifacts/evidence/tests.json; exit $$test_exit_code; }
	./bin/amendsctl core-demo > artifacts/evidence/core-demo.txt
	./bin/amendsctl sim -seed 1 -trace artifacts/evidence/seed-1.json > artifacts/evidence/seed-1-report.json
	./bin/amendsctl sim-delivery -seed 17 -trace artifacts/evidence/delivery-seed-17.json > artifacts/evidence/delivery-seed-17-report.json
	./bin/amendsctl sim-worker -seed 17 -trace artifacts/evidence/worker-seed-17.json > artifacts/evidence/worker-seed-17-report.json

deps-config:
	$(COMPOSE) config --quiet

up:
	$(COMPOSE) up --wait --wait-timeout 120 postgres redpanda

ready:
	$(COMPOSE) exec -T postgres sh -c 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -X -v ON_ERROR_STOP=1 -Atqc "SELECT 1"'
	$(COMPOSE) exec -T redpanda timeout 30s rpk cluster health --exit-when-healthy -X admin.hosts=127.0.0.1:9644
	$(COMPOSE) exec -T redpanda rpk cluster info -X brokers=redpanda:9092

status:
	$(COMPOSE) ps --all

down:
	$(COMPOSE) stop --timeout 30 postgres redpanda
