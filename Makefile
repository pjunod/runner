# nzbd — developer Makefile.
#
# Fresh clone, get productive:
#   make setup      # install the toolchain, PP tools and git hooks
#   make run        # build + run the daemon (first-run setup UI on :6789)
#   make check      # core Rust gates: fmt + clippy + tests + MSRV
#
# Run `make` (or `make help`) to list every target.

CARGO   ?= cargo
RUSTUP  ?= rustup
RUSTUP_PATH_PREFIX ?= $(if $(shell command -v $(RUSTUP)),$(dir $(shell command -v $(RUSTUP))):,)
# The daemon binary package (cargo -p nzbd).
DAEMON  := nzbd
# Minimum supported Rust (keep in sync with Cargo.toml rust-version).
MSRV    := 1.95
FUZZ_TOOLCHAIN ?= nightly-2026-08-01
FUZZ_TARGET ?=
FUZZ_RUNS ?= 20000
FUZZ_MAX_LEN ?= 1048576
FUZZ_MAGNET_MAX_LEN ?= 32768
FUZZ_SECONDS ?=
UNAME_S := $(shell uname -s)
# Build identity for container builds. The Docker context excludes .git
# (see .dockerignore), so an image cannot derive its own commit — it has
# to be passed in, or the daemon reports `<version>+unknown`.
GIT_DESCRIBE := $(shell git describe --tags --always --dirty --abbrev=9 --match='v[0-9]*' 2>/dev/null)

# Optional overrides for `make run`, e.g.
#   make run CONFIG=dev/config/nzbd.toml BIND=0.0.0.0:6789
CONFIG  ?=
BIND    ?=
RUN_ARGS := run
ifneq ($(strip $(CONFIG)),)
RUN_ARGS += --config $(CONFIG)
endif
ifneq ($(strip $(BIND)),)
RUN_ARGS += --bind $(BIND)
endif

.DEFAULT_GOAL := help

##@ Help

.PHONY: help
help: ## List all targets
	@awk 'BEGIN {FS = ":.*##"} \
		/^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next} \
		/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' \
		$(MAKEFILE_LIST)

##@ Toolchain & setup

.PHONY: setup
setup: toolchain tools hooks ## One-shot dev setup: toolchain + PP tools + git hooks
	@echo "OK - dev environment ready; try 'make run' or 'make check'"

.PHONY: toolchain
toolchain: ## Rust components (fmt, clippy, llvm-tools) + MSRV toolchain + cargo-llvm-cov
	rustup component add rustfmt clippy llvm-tools-preview
	rustup toolchain install $(MSRV) --profile minimal
	$(CARGO) install cargo-llvm-cov --locked || true

.PHONY: tools
tools: ## Install the post-processing tools the tests exercise (par2, 7z)
ifeq ($(UNAME_S),Darwin)
	brew install par2 p7zip
else ifeq ($(UNAME_S),Linux)
	@if command -v apt-get >/dev/null 2>&1; then \
		sudo apt-get update && sudo apt-get install -y par2 p7zip-full; \
	else \
		echo "Install 'par2' and '7z' with your package manager (non-apt distro)."; \
	fi
else
	@echo "Install 'par2' and '7z' manually on $(UNAME_S)."
endif

.PHONY: hooks
hooks: ## Point git at the committed pre-commit / pre-push hooks
	git config core.hooksPath .githooks
	@echo "OK - hooks enabled (pre-commit: fmt; pre-push: clippy + tests)"

##@ Build & run

.PHONY: build
build: ## Debug build of the daemon
	$(CARGO) build -p $(DAEMON)

.PHONY: release
release: ## Optimized release build of the daemon
	$(CARGO) build --release -p $(DAEMON)

.PHONY: run
run: ## Run the daemon (CONFIG=... BIND=... optional; no config -> first-run setup UI)
	$(CARGO) run -p $(DAEMON) -- $(RUN_ARGS)

.PHONY: docker
docker: ## Build the container from the working tree and run it (dev/ compose)
	cd dev && NZBD_GIT_DESCRIBE=$(GIT_DESCRIBE) docker compose up --build

.PHONY: docker-build
docker-build: ## Build the image only, stamped with this checkout's identity
	docker build --build-arg NZBD_GIT_DESCRIBE=$(GIT_DESCRIBE) -t nzbd .
	@echo "OK - built nzbd:latest as $(GIT_DESCRIBE)"

.PHONY: version
version: ## Print the build identity this checkout would stamp into an image
	@echo "$(GIT_DESCRIBE)"

##@ Test & quality gates

.PHONY: test
test: ## Whole workspace test suite (unit + e2e + cluster + daemon + UI boot)
	$(CARGO) test --workspace

.PHONY: test-strict
test-strict: ## Like `test`, but a missing par2/7z is a failure, not a skip (as in CI)
	NZBD_REQUIRE_TOOLS=1 $(CARGO) test --workspace

.PHONY: ui-test
ui-test: ## Embedded UI boot and stable-DOM regression harnesses
	node crates/nzbd/tests/ui_boot_harness.js crates/nzbd-api/ui/index.html
	node crates/nzbd/tests/ui_dom_harness.js crates/nzbd-api/ui/index.html

.PHONY: fmt
fmt: ## Format the whole workspace
	$(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting without writing (CI gate)
	$(CARGO) fmt --all --check

.PHONY: lint
lint: ## Clippy across all targets with warnings denied (CI gate)
	$(CARGO) clippy --workspace --all-targets -- -D warnings

.PHONY: msrv
msrv: ## Type-check on the minimum supported Rust (1.95)
	$(RUSTUP) run $(MSRV) $(CARGO) check --workspace --all-targets

.PHONY: coverage
coverage: ## Line-coverage summary (needs cargo-llvm-cov; `make toolchain` installs it)
	$(CARGO) llvm-cov --workspace --no-fail-fast --summary-only

.PHONY: coverage-html
coverage-html: ## Full HTML coverage report under target/llvm-cov/html/
	$(CARGO) llvm-cov --workspace --no-fail-fast --html
	@echo "report: target/llvm-cov/html/index.html"

.PHONY: fuzz-deps
fuzz-deps: ## Verify the isolated BitTorrent fuzz dependency graph
	scripts/check-bittorrent-fuzz-deps.sh

.PHONY: fuzz-test
fuzz-test: fuzz-deps ## Verify the committed BitTorrent fuzz seed classes
	$(CARGO) test --manifest-path fuzz/Cargo.toml --locked

.PHONY: fuzz-metainfo
fuzz-metainfo: fuzz-test fuzz-metainfo-run ## Coverage-guided BitTorrent metainfo preflight smoke

.PHONY: fuzz-metainfo-run
fuzz-metainfo-run:
	mkdir -p fuzz/corpus/metainfo_preflight
	cd fuzz && PATH="$(RUSTUP_PATH_PREFIX)$$PATH" $(CARGO) +$(FUZZ_TOOLCHAIN) \
		fuzz run $(if $(strip $(FUZZ_TARGET)),--target $(FUZZ_TARGET),) \
		metainfo_preflight \
		corpus/metainfo_preflight seeds/metainfo_preflight -- \
		$(if $(strip $(FUZZ_SECONDS)),-max_total_time=$(FUZZ_SECONDS),-runs=$(FUZZ_RUNS)) \
		-max_len=$(FUZZ_MAX_LEN) -dict=dictionaries/metainfo.dict

.PHONY: fuzz-magnet
fuzz-magnet: fuzz-test fuzz-magnet-run ## Coverage-guided BitTorrent magnet preflight smoke

.PHONY: fuzz-magnet-run
fuzz-magnet-run:
	mkdir -p fuzz/corpus/magnet_preflight
	cd fuzz && PATH="$(RUSTUP_PATH_PREFIX)$$PATH" $(CARGO) +$(FUZZ_TOOLCHAIN) \
		fuzz run $(if $(strip $(FUZZ_TARGET)),--target $(FUZZ_TARGET),) \
		magnet_preflight \
		corpus/magnet_preflight seeds/magnet_preflight -- \
		$(if $(strip $(FUZZ_SECONDS)),-max_total_time=$(FUZZ_SECONDS),-runs=$(FUZZ_RUNS)) \
		-max_len=$(FUZZ_MAGNET_MAX_LEN) -dict=dictionaries/magnet.dict

.PHONY: bittorrent-policy
bittorrent-policy: ## Verify adapter, daemon, review-doc, and dependency policies
	bash -n scripts/check-bittorrent-storage-full-probe.sh
	RQBIT_SERIES_DERIVE_ONLY=1 scripts/check-rqbit-maintained-patch-series.sh
	scripts/check-bittorrent-deps.sh
	scripts/check-bittorrent-release-review.sh
	scripts/check-reviewed-dependency-exceptions.sh

.PHONY: check
check: fmt-check lint test msrv ## Core Rust gates (run before pushing)
	@echo "OK - all local gates passed"

.PHONY: fast-check-rust
fast-check-rust: ## Reviewed merge candidate: compile/lint Rust and run bounded cluster regressions
	$(CARGO) fmt --all --check
	$(CARGO) check --workspace --all-targets --locked
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings
	$(CARGO) test --manifest-path vendor/hiqlite/Cargo.toml --target-dir "$${CARGO_TARGET_DIR:-$(CURDIR)/target}" --locked --no-default-features --features sqlite --lib api_stream_stop_watch_ignores_live_updates_and_observes_terminal_states
	$(CARGO) test --locked -p nzbd-cluster --lib
	$(CARGO) test --locked -p nzbd-cluster --test cluster_e2e -- --test-threads=1

.PHONY: fast-check-torrent
fast-check-torrent: ## Reviewed torrent changes: bounded transfer, recovery, and release-dispatch checks
	@status=0; \
	$(CARGO) test --locked -p nzbd-engine --lib torrent_ || status=$$?; \
	$(CARGO) test --locked -p nzbd-engine --lib owner::tests::seed_policy_ || status=$$?; \
	$(CARGO) test --locked --no-fail-fast -p nzbd-api -p nzbd-config -p nzbd-types -p nzbd-state -p nzbd-qbit-compat --lib || status=$$?; \
	$(CARGO) test --locked -p nzbd-engine --test e2e single_node_startup_restores_torrent_rows_before_backend_attachment -- --exact || status=$$?; \
	$(CARGO) test --locked -p nzbd-engine --test e2e cluster_authority_adoption_refuses_dormant_torrent_rows_without_rewrite -- --exact || status=$$?; \
	RQBIT_SERIES_DISPATCH_ONLY=1 scripts/check-rqbit-maintained-patch-series.sh || status=$$?; \
	exit $$status

.PHONY: fast-check-startup
fast-check-startup: ## Startup recovery writer, admission, enrollment, and daemon regressions
	@status=0; \
	$(CARGO) test --locked -p nzbd-engine --lib writer::tests || status=$$?; \
	$(CARGO) test --locked -p nzbd-engine --lib recovery:: || status=$$?; \
	$(CARGO) test --locked -p nzbd-engine --lib owner_deadline_tests || status=$$?; \
	$(CARGO) test --locked -p nzbd-engine --lib owner:: -- --skip torrent_ --skip seed_policy_ || status=$$?; \
	$(CARGO) test --locked -p nzbd-post --lib admission_tests || status=$$?; \
	$(CARGO) test --locked -p nzbd-post --lib terminal_reads_retry_timeouts_and_use_the_live_keep_setting || status=$$?; \
	$(CARGO) test --locked -p nzbd-post --test pp_pipeline legacy_repair_timeout_recovers_automatically_and_records_terminal_failure || status=$$?; \
	$(CARGO) test --locked -p nzbd-state --test enrollment || status=$$?; \
	$(CARGO) test --locked -p nzbd --bin nzbd || status=$$?; \
	$(CARGO) test --locked -p nzbd --test daemon -- --test-threads=1 || status=$$?; \
	exit $$status

.PHONY: fast-check
fast-check: ## Final affected fast lane; run once after the combined adversarial review
	scripts/check-fast-lane-contract
	$(MAKE) ui-test
	$(MAKE) fast-check-rust
	$(MAKE) fast-check-torrent
	$(MAKE) fast-check-startup
	@echo "OK - merge-candidate fast lane passed"

.PHONY: gate
gate: ## Deterministic release gate: core checks + BitTorrent policy + fuzz contracts
	$(MAKE) check
	$(MAKE) bittorrent-policy
	$(MAKE) fuzz-test
	@echo "OK - deterministic release gate passed"

.PHONY: gate-fuzz
gate-fuzz: ## Release gate plus both bounded BitTorrent libFuzzer campaigns
	$(MAKE) gate
	$(MAKE) fuzz-metainfo-run
	$(MAKE) fuzz-magnet-run
	@echo "OK - deterministic and bounded fuzz release gates passed"

##@ Housekeeping

.PHONY: clean
clean: ## Remove build artifacts (cargo clean)
	$(CARGO) clean
