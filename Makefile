# Makefile for gearman-go.
#
# Two things about this repo that the targets below encode, so you do not have
# to remember them:
#
#  1. example/ is a SEPARATE module. Root ./... does not reach it, so the
#     examples are the only compile-time check that a public API change has not
#     broken a caller. `make examples` runs it.
#  2. Custom test flags must come AFTER the package list. `go test -integration
#     ./client` silently tests the current directory and exits 0 — it looks
#     like a pass and verifies nothing. Every target here puts the flag last.

GO      ?= go
PKGS    ?= ./...
TIMEOUT ?= 120s

GEARMAND_HOST  ?= 127.0.0.1
GEARMAND_PORT  ?= 4730
# Pinned, not :latest, so a run is reproducible. 2.1.0, 2.1.0-alpine and latest
# are currently the same image id — the upstream image is already Alpine-based.
GEARMAND_IMAGE ?= artefactual/gearmand:2.1.0-alpine
GEARMAND_NAME  ?= gearman-go-it

port_open = bash -c 'exec 3<>/dev/tcp/$(GEARMAND_HOST)/$(GEARMAND_PORT)' 2>/dev/null

# Pre-existing `go vet` findings, present before any of this work. The vet
# target ignores exactly these and fails on anything new. Matched by file and
# message, not line: editing either file shifts the line and would otherwise
# look like a brand new finding.
VET_BASELINE := client/client\.go:[0-9]+:[0-9]+: unreachable code|client/pool_test\.go:[0-9]+:[0-9]+: unreachable code

.DEFAULT_GOAL := help
.PHONY: help build test race vet fmt fmt-check examples knownbugs reproducers \
        integration gearmand gearmand-stop tidy clean check ci

help: ## Show this help
	@echo "gearman-go targets:"
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-14s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Expected to FAIL until the client races are fixed: race, knownbugs, reproducers."

build: ## Compile the library
	$(GO) build $(PKGS)

test: ## Run the default test suite (no gearmand needed)
	$(GO) test -count=1 -timeout $(TIMEOUT) $(PKGS)

vet: ## Run go vet, ignoring the pre-existing baseline findings
	@out=$$($(GO) vet $(PKGS) 2>&1 | grep -Ev '$(VET_BASELINE)' || true); \
	if [ -n "$$out" ]; then \
	  echo "$$out"; \
	  echo "make: vet reported findings beyond the known baseline"; \
	  exit 1; \
	fi; \
	echo "vet: clean (2 pre-existing baseline findings ignored)"
	@cd example && $(GO) vet ./... && echo "vet: example module clean"

fmt: ## Format all Go files (both modules)
	gofmt -w .

fmt-check: ## Fail if any Go file needs formatting
	@bad=$$(gofmt -l . 2>/dev/null); \
	if [ -n "$$bad" ]; then echo "needs gofmt:"; echo "$$bad"; exit 1; fi; \
	echo "gofmt: clean"

examples: ## Build the example module against the working tree
	cd example && $(GO) build ./...

check: build vet fmt-check test examples ## Everything that must pass today
	@echo "check: OK"

ci: check ## Alias for check

# --- targets that are expected to fail while the defects are unfixed --------

race: ## Full suite under -race (FAILS today: the client has known races)
	$(GO) test -race -count=1 -timeout $(TIMEOUT) $(PKGS)

reproducers: ## The three documented race reproducers (expected FAIL)
	$(GO) test -race -count=1 -timeout $(TIMEOUT) ./client -run TestRace

# No -run filter on purpose: the -knownbugs gate already selects them, so new
# entries in knownbugs_test.go are picked up without editing this Makefile.
knownbugs: ## Tests for known unfixed defects (expected FAIL)
	$(GO) test -count=1 -timeout $(TIMEOUT) ./client -knownbugs

# --- occasional ------------------------------------------------------------

integration: ## Run the integration suite, starting gearmand in docker if needed
	@set -e; \
	started=0; \
	if $(port_open); then \
	  echo "==> using the gearmand already on $(GEARMAND_HOST):$(GEARMAND_PORT)"; \
	else \
	  command -v docker >/dev/null 2>&1 || { \
	    echo "make: nothing on $(GEARMAND_HOST):$(GEARMAND_PORT) and no docker to start one."; \
	    echo "      Start a job server yourself, or set GEARMAND_HOST/GEARMAND_PORT."; \
	    exit 1; }; \
	  echo "==> starting $(GEARMAND_IMAGE) as $(GEARMAND_NAME)"; \
	  docker rm -f $(GEARMAND_NAME) >/dev/null 2>&1 || true; \
	  docker run --rm -d --name $(GEARMAND_NAME) \
	    -p $(GEARMAND_PORT):4730 $(GEARMAND_IMAGE) >/dev/null; \
	  started=1; \
	  for i in $$(seq 60); do $(port_open) && break; sleep 0.25; done; \
	  $(port_open) || { \
	    echo "make: $(GEARMAND_NAME) never accepted connections; logs follow"; \
	    docker logs $(GEARMAND_NAME) 2>&1 | tail -20; \
	    docker rm -f $(GEARMAND_NAME) >/dev/null 2>&1 || true; \
	    exit 1; }; \
	fi; \
	rc=0; \
	$(GO) test -count=1 -timeout $(TIMEOUT) ./client ./worker -integration || rc=$$?; \
	if [ $$started -eq 1 ]; then \
	  echo "==> stopping $(GEARMAND_NAME)"; \
	  docker rm -f $(GEARMAND_NAME) >/dev/null 2>&1 || true; \
	fi; \
	exit $$rc

gearmand: ## Start a background gearmand for manual use
	@docker rm -f $(GEARMAND_NAME) >/dev/null 2>&1 || true
	docker run --rm -d --name $(GEARMAND_NAME) -p $(GEARMAND_PORT):4730 $(GEARMAND_IMAGE)
	@for i in $$(seq 60); do $(port_open) && break; sleep 0.25; done
	@$(port_open) && echo "gearmand ready on $(GEARMAND_HOST):$(GEARMAND_PORT)"

gearmand-stop: ## Stop the gearmand started by `make gearmand`
	@docker rm -f $(GEARMAND_NAME) >/dev/null 2>&1 && echo "stopped" || echo "not running"

tidy: ## Tidy both modules
	$(GO) mod tidy
	cd example && $(GO) mod tidy

clean: ## Remove build artifacts and the test cache
	$(GO) clean -testcache
	$(GO) clean $(PKGS)
	cd example && $(GO) clean ./...
	rm -f example/client/client example/worker/worker
