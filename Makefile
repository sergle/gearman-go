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

# Benchmark knobs. BENCHTIME is a fixed iteration count rather than a duration
# so a run is comparable across machines and across a code change; BENCHCOUNT
# is above 1 on purpose, because a single run is not evidence for anything
# concurrent -- the spread between runs is wider than the effects measured.
BENCH         ?= .
BENCHTIME     ?= 2000x
BENCHCOUNT    ?= 10
BENCH_TIMEOUT ?= 600s

GEARMAND_HOST  ?= 127.0.0.1
GEARMAND_PORT  ?= 4730
# Job servers to bring up, on GEARMAND_PORT, +1, +2 ... The first is what every
# single-server test connects to; the rest feed the multi-server Pool tests in
# client/pool_integration_test.go via GEARMAND_POOL_ADDRS. 1 is the old behaviour.
GEARMAND_COUNT ?= 3
# Pinned, not :latest, so a run is reproducible. 2.1.0, 2.1.0-alpine and latest
# are currently the same image id — the upstream image is already Alpine-based.
GEARMAND_IMAGE ?= artefactual/gearmand:2.1.0-alpine
GEARMAND_NAME  ?= gearman-go-it

# port_open takes the port as $1. Double quotes are required -- single ones
# leave the inner shell a literal $1, so every check fails. `bash -c` is
# explicit because make's default /bin/sh has no /dev/tcp.
define port_open_fn
port_open() { bash -c "exec 3<>/dev/tcp/$(GEARMAND_HOST)/$$1" 2>/dev/null; }
endef

# Pre-existing `go vet` findings, present before any of this work. The vet
# target ignores exactly these and fails on anything new. Matched by file and
# message, not line: editing either file shifts the line and would otherwise
# look like a brand new finding.
#
# `^#` drops go's own package headers ("# github.com/sergle/gearman-go/client"
# and the bracketed test-variant line beside it); they are not findings, and
# leaving them in made the target fail with the baseline itself as the output.
VET_BASELINE := ^#|client/client\.go:[0-9]+:[0-9]+: unreachable code|client/pool_test\.go:[0-9]+:[0-9]+: unreachable code

.DEFAULT_GOAL := help
.PHONY: help build test bench race vet fmt fmt-check examples knownbugs \
        reproducers integration gearmand gearmand-stop tidy clean check ci

help: ## Show this help
	@echo "gearman-go targets:"
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-14s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "race, knownbugs and reproducers exist to FAIL while the defects they"
	@echo "describe are open; bench fails the cases whose defect wedges them."
	@echo "Read the per-test output, not the exit code -- which cases fail is the"
	@echo "signal, and it changes as fixes land. Each test comments its own defect."

build: ## Compile the library
	$(GO) build $(PKGS)

test: ## Run the default test suite (no gearmand needed)
	$(GO) test -count=1 -timeout $(TIMEOUT) $(PKGS)

# -run xxx matches no test, so only the benchmarks run. Deliberately not part of
# check: `go test` skips benchmarks unless -bench is given, so the new files
# cost the default suite nothing but a compile.
#
# Every case reports a number today. BenchmarkClientMixedDoAndEcho used to wedge
# and fail its watchdog: Echo wrote to the shared bufio.Writer unlocked, then
# waited on a latch with no timeout. It now measures the serialisation its fix
# introduced -- Do and Echo no longer overlap.
bench: ## Run the benchmarks against the in-process fake servers (no gearmand)
	$(GO) test -run xxx -bench '$(BENCH)' -benchtime $(BENCHTIME) \
	  -count $(BENCHCOUNT) -benchmem -timeout $(BENCH_TIMEOUT) ./client ./worker

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
# entries in either knownbugs_test.go are picked up without editing this
# Makefile. Both packages define the flag; passing it to a package that did not
# would fail with "flag provided but not defined".
knownbugs: ## Tests for known unfixed defects (expected FAIL)
	$(GO) test -count=1 -timeout $(TIMEOUT) ./client ./worker -knownbugs

# --- occasional ------------------------------------------------------------

# A port already answering is reused; only containers this run started are torn
# down. An extra server that cannot be started is a warning -- the pool tests
# skip on fewer than two. The FIRST is fatal: every other test hardcodes it.
#
# `docker run` is tested, not left bare, and cleanup is a trap: under `set -e` a
# bare failure at i=2 would abort before i=1's container was recorded, leaving it
# on 4730 for the next run to silently "reuse". The trap also covers Ctrl-C.
integration: ## Run the integration suite, starting GEARMAND_COUNT gearmands in docker if needed
	@set -e; \
	$(port_open_fn); \
	started=""; addrs=""; \
	cleanup() { \
	  for n in $$started; do \
	    echo "==> stopping $$n"; \
	    docker rm -f $$n >/dev/null 2>&1 || true; \
	  done; \
	  started=""; \
	}; \
	trap cleanup EXIT INT TERM; \
	for i in $$(seq $(GEARMAND_COUNT)); do \
	  port=$$(($(GEARMAND_PORT) + i - 1)); name=$(GEARMAND_NAME)-$$i; ok=0; \
	  if port_open $$port; then \
	    echo "==> using the job server already on $(GEARMAND_HOST):$$port"; ok=1; \
	  elif command -v docker >/dev/null 2>&1; then \
	    echo "==> starting $(GEARMAND_IMAGE) as $$name on $(GEARMAND_HOST):$$port"; \
	    docker rm -f $$name >/dev/null 2>&1 || true; \
	    if docker run --rm -d --name $$name --hostname $$name \
	         -p $$port:4730 $(GEARMAND_IMAGE) >/dev/null; then \
	      started="$$started $$name"; \
	      for n in $$(seq 60); do port_open $$port && break; sleep 0.25; done; \
	      if port_open $$port; then ok=1; else \
	        echo "make: $$name never accepted connections; logs follow"; \
	        docker logs $$name 2>&1 | tail -20; \
	      fi; \
	    else \
	      echo "make: could not start $$name"; \
	      docker rm -f $$name >/dev/null 2>&1 || true; \
	    fi; \
	  fi; \
	  if [ $$ok -eq 1 ]; then \
	    addrs="$${addrs:+$$addrs,}$(GEARMAND_HOST):$$port"; \
	  elif [ $$i -eq 1 ]; then \
	    echo "make: no job server on $(GEARMAND_HOST):$$port and none could be started."; \
	    echo "      Start one yourself, or set GEARMAND_HOST/GEARMAND_PORT."; \
	    exit 1; \
	  else \
	    echo "make: no job server on $(GEARMAND_HOST):$$port; the multi-server Pool tests will skip"; \
	  fi; \
	done; \
	echo "==> job servers: $$addrs"; \
	rc=0; \
	GEARMAND_POOL_ADDRS="$$addrs" \
	  $(GO) test -count=1 -timeout $(TIMEOUT) ./client ./worker -integration || rc=$$?; \
	exit $$rc

gearmand: ## Start GEARMAND_COUNT background gearmands for manual use
	@set -e; \
	$(port_open_fn); \
	for i in $$(seq $(GEARMAND_COUNT)); do \
	  port=$$(($(GEARMAND_PORT) + i - 1)); name=$(GEARMAND_NAME)-$$i; \
	  docker rm -f $$name >/dev/null 2>&1 || true; \
	  docker run --rm -d --name $$name --hostname $$name \
	    -p $$port:4730 $(GEARMAND_IMAGE) >/dev/null; \
	  for n in $$(seq 60); do port_open $$port && break; sleep 0.25; done; \
	  port_open $$port \
	    && echo "$$name ready on $(GEARMAND_HOST):$$port" \
	    || { echo "make: $$name never accepted connections"; exit 1; }; \
	done

# The unsuffixed name too: containers left by the single-server version.
gearmand-stop: ## Stop the gearmands started by `make gearmand`
	@stopped=""; \
	for name in $(GEARMAND_NAME) $$(for i in $$(seq $(GEARMAND_COUNT)); do echo $(GEARMAND_NAME)-$$i; done); do \
	  [ -n "$$(docker ps -aq -f name=^$$name$$ 2>/dev/null)" ] || continue; \
	  docker rm -f $$name >/dev/null 2>&1 && stopped="$$stopped $$name" || true; \
	done; \
	if [ -n "$$stopped" ]; then echo "stopped:$$stopped"; else echo "not running"; fi

tidy: ## Tidy both modules
	$(GO) mod tidy
	cd example && $(GO) mod tidy

clean: ## Remove build artifacts and the test cache
	$(GO) clean -testcache
	$(GO) clean $(PKGS)
	cd example && $(GO) clean ./...
	rm -f example/client/client example/worker/worker
