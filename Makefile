# ax-cache development Makefile.
#
# Run `make help` to list everything.
# Override variables on the command line, e.g. `make bench BENCH=zipfian`.

CARGO ?= cargo
PKG := ax-cache

# Use bash with pipefail so a failure in `cargo bench` propagates through
# the `| tee result.txt` pipelines below instead of being masked by tee.
SHELL := bash
.SHELLFLAGS := -eo pipefail -c

.DEFAULT_GOAL := help

##@ General

.PHONY: help
help: ## Print this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z][a-zA-Z0-9_-]*:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

.PHONY: version
version: ## Print crate version
	@grep -E '^version =' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/Crate version: \1/'

##@ Build

.PHONY: build
build: ## Release build
	$(CARGO) build --release

.PHONY: check
check: ## Fast cargo check across all targets
	$(CARGO) check --all-targets

##@ Test

.PHONY: test
test: ## Run all tests (debug)
	$(CARGO) test

.PHONY: test-release
test-release: ## Run all tests in release mode
	$(CARGO) test --release

.PHONY: test-lib
test-lib: ## Run library tests only
	$(CARGO) test --lib

##@ Quality

.PHONY: fmt
fmt: ## Format all code with rustfmt
	$(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting without modifying files
	$(CARGO) fmt --all -- --check

.PHONY: lint
lint: ## Run clippy on the library (denies warnings)
	$(CARGO) clippy --lib -- -D warnings

.PHONY: lint-all
lint-all: ## Run clippy on everything including benches & tests
	$(CARGO) clippy --all-targets -- -D warnings

##@ Benchmarks

# Available benches (declared in Cargo.toml):
#   single_thread, zipfian, contention, scan_resistance,
#   head_to_head_moka_single, head_to_head_moka_contention,
#   head_to_head_moka_zipfian, soak_test
#
# All bench targets stream output to both the terminal AND $(RESULT)
# (default: result.txt) for easier offline evaluation. Override with
# `make bench RESULT=runs/2026-05-29.txt` if you want to keep history.

BENCH  ?=
RESULT ?= result.txt

.PHONY: bench
bench: ## Run all benchmarks (~minutes). Use BENCH=<name> to filter. Output -> $(RESULT)
ifeq ($(strip $(BENCH)),)
	$(CARGO) bench 2>&1 | tee $(RESULT)
else
	$(CARGO) bench --bench $(BENCH) 2>&1 | tee $(RESULT)
endif

.PHONY: bench-single
bench-single: ## single_thread benchmark -> $(RESULT)
	$(CARGO) bench --bench single_thread 2>&1 | tee $(RESULT)

.PHONY: bench-zipfian
bench-zipfian: ## zipfian benchmark -> $(RESULT)
	$(CARGO) bench --bench zipfian 2>&1 | tee $(RESULT)

.PHONY: bench-contention
bench-contention: ## contention benchmark -> $(RESULT)
	$(CARGO) bench --bench contention 2>&1 | tee $(RESULT)

.PHONY: bench-scan
bench-scan: ## scan_resistance benchmark -> $(RESULT)
	$(CARGO) bench --bench scan_resistance 2>&1 | tee $(RESULT)

.PHONY: bench-moka
bench-moka: ## All head-to-head moka benchmarks -> $(RESULT)
	$(CARGO) bench --bench head_to_head_moka_single       2>&1 | tee    $(RESULT)
	$(CARGO) bench --bench head_to_head_moka_contention   2>&1 | tee -a $(RESULT)
	$(CARGO) bench --bench head_to_head_moka_zipfian      2>&1 | tee -a $(RESULT)

.PHONY: bench-soak
bench-soak: ## soak_test benchmark -> $(RESULT)
	$(CARGO) bench --bench soak_test 2>&1 | tee $(RESULT)

.PHONY: diagnostics
diagnostics: ## Run the property-diagnostic harness (eviction, sharding, TTL, contention, memory, fairness, stale) -> $(RESULT)
	$(CARGO) run --release --example diagnostics 2>&1 | tee $(RESULT)

.PHONY: bench-report
bench-report: ## Open last criterion HTML report
	@if [ -f target/criterion/report/index.html ]; then \
		(command -v open >/dev/null && open target/criterion/report/index.html) || \
		(command -v xdg-open >/dev/null && xdg-open target/criterion/report/index.html) || \
		echo "Open manually: target/criterion/report/index.html"; \
	else \
		echo "No criterion report found. Run 'make bench' first."; \
	fi

##@ Documentation

.PHONY: doc
doc: ## Build rustdoc and open in browser
	$(CARGO) doc --no-deps --open

.PHONY: doc-build
doc-build: ## Build rustdoc without opening
	$(CARGO) doc --no-deps

##@ CI simulation

.PHONY: ci
ci: fmt-check lint test ## Simulate CI locally (fmt + lint + test)
	@echo ""
	@echo "CI checks passed locally."

.PHONY: pre-release
pre-release: ci test-release ## Comprehensive pre-release validation
	@echo ""
	@echo "Pre-release validation complete:"
	@echo "  - fmt-check OK"
	@echo "  - clippy OK"
	@echo "  - tests pass (debug + release)"
	@echo ""
	@echo "Next steps:"
	@echo "  make dry-publish   # Verify crates.io publish would work"
	@echo "  git tag v$$(grep -E '^version =' Cargo.toml | head -1 | sed 's/version = \"\\(.*\\)\"/\\1/')"
	@echo "  git push --tags"

##@ Release

.PHONY: dry-publish
dry-publish: ## cargo publish --dry-run
	$(CARGO) publish -p $(PKG) --dry-run

.PHONY: publish
publish: ## cargo publish (real)
	$(CARGO) publish -p $(PKG)

##@ Maintenance

.PHONY: clean
clean: ## Remove entire target directory
	$(CARGO) clean

.PHONY: clean-bench
clean-bench: ## Remove criterion artifacts only
	rm -rf target/criterion

.PHONY: update
update: ## Update Cargo.lock to latest compatible versions
	$(CARGO) update

.PHONY: tree
tree: ## Show direct dependency tree
	$(CARGO) tree --depth 1
