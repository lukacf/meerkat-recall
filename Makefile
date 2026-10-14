# Elephant CI/CD Makefile
# Single source of truth for build, test, hook, CI, and release commands.

GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

CARGO ?= ./scripts/repo-cargo
HOOK_CARGO ?= ./scripts/hook-cargo
NEXTEST_ARGS ?=
HOOK_UNIT_ENV ?= RUST_LANE_ID=hook-unit
HOOK_FULL_ENV ?= RUST_LANE_ID=hook-full
HOOK_LINT_ENV ?= RUST_LANE_ID=hook-clippy

.PHONY: all build release test test-unit test-all hook-test hook-test-all hook-lint test-e2e test-e2e-model test-integration test-scenarios lint fmt fmt-check audit audit-alt ci ci-smoke clean doc docs-check doc-open coverage check verify-surface verify-version verify-version-parity release-preflight publish-dry-run install-hooks uninstall-hooks pre-commit-all update outdated bench cargo-bench run-api run-pipeline xtask help

all: ci

build:
	@echo "$(GREEN)Building Elephant workspace...$(NC)"
	$(CARGO) build --workspace

release:
	@echo "$(GREEN)Building release version...$(NC)"
	$(CARGO) build --workspace --release

test:
	@echo "$(GREEN)Running fast tests...$(NC)"
	@if $(CARGO) nextest --version >/dev/null 2>&1; then \
		$(CARGO) nextest run --workspace --show-progress none --status-level none --final-status-level fail $(NEXTEST_ARGS); \
	else \
		$(CARGO) test --workspace $(NEXTEST_ARGS); \
	fi

test-unit:
	@echo "$(GREEN)Running unit tests...$(NC)"
	@if $(CARGO) nextest --version >/dev/null 2>&1; then \
		$(CARGO) nextest run --workspace --lib --bins --show-progress none --status-level none --final-status-level fail $(NEXTEST_ARGS); \
	else \
		$(CARGO) test --workspace --lib --bins $(NEXTEST_ARGS); \
	fi

hook-test:
	@echo "$(GREEN)Running hook unit tests...$(NC)"
	$(HOOK_UNIT_ENV) ./scripts/pre-push-unit.sh

test-all:
	@echo "$(GREEN)Running full test suite...$(NC)"
	@if $(CARGO) nextest --version >/dev/null 2>&1; then \
		$(CARGO) nextest run --workspace --all-features $(NEXTEST_ARGS); \
	else \
		$(CARGO) test --workspace --all-features $(NEXTEST_ARGS); \
	fi

hook-test-all:
	@echo "$(GREEN)Running full hook test suite...$(NC)"
	@if $(HOOK_FULL_ENV) $(HOOK_CARGO) nextest --version >/dev/null 2>&1; then \
		$(HOOK_FULL_ENV) $(HOOK_CARGO) nextest run --workspace --all-features $(NEXTEST_ARGS); \
	else \
		$(HOOK_FULL_ENV) $(HOOK_CARGO) test --workspace --all-features $(NEXTEST_ARGS); \
	fi

test-e2e-model:
	@echo "$(GREEN)Running explicitly provisioned local-model regression...$(NC)"
	$(CARGO) test-e2e-model

test-e2e:
	@echo "$(GREEN)Running E2E tests...$(NC)"
	$(CARGO) test-e2e

test-integration:
	@echo "$(GREEN)Running integration tests...$(NC)"
	$(CARGO) test --workspace -- --ignored

test-scenarios:
	@echo "$(GREEN)Running scenario tests...$(NC)"
	$(CARGO) test -p elephant-scenarios -- --nocapture

lint:
	@echo "$(GREEN)Running clippy...$(NC)"
	$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings

hook-lint:
	@echo "$(GREEN)Running hook clippy...$(NC)"
	$(HOOK_LINT_ENV) $(HOOK_CARGO) clippy --workspace --all-targets --all-features -- -D warnings

fmt-check:
	@echo "$(GREEN)Checking formatting...$(NC)"
	$(CARGO) fmt --all -- --check

fmt:
	@echo "$(GREEN)Fixing formatting...$(NC)"
	$(CARGO) fmt --all

audit:
	@echo "$(GREEN)Running security audit...$(NC)"
	$(CARGO) deny check

audit-alt:
	@echo "$(GREEN)Running cargo-audit...$(NC)"
	$(CARGO) audit

verify-surface:
	@echo "$(GREEN)Verifying active surface and legacy drift...$(NC)"
	bash scripts/verify_active_surface.sh
	bash scripts/verify_legacy_terms.sh

verify-version-parity:
	@echo "$(GREEN)Verifying version parity...$(NC)"
	bash scripts/verify-version-parity.sh

ci: fmt-check verify-surface verify-version-parity lint test-all test-e2e audit doc
	@echo "$(GREEN)CI pipeline complete!$(NC)"

ci-smoke: fmt-check verify-surface verify-version-parity lint test-unit audit
	@echo "$(GREEN)CI smoke pipeline complete!$(NC)"

check:
	@echo "$(GREEN)Running cargo check...$(NC)"
	$(CARGO) check --workspace --all-targets --all-features

doc:
	@echo "$(GREEN)Generating documentation...$(NC)"
	$(CARGO) doc --workspace --no-deps --all-features

# Install validation dependencies with npm ci --prefix scripts/docs --ignore-scripts.
docs-check:
	npm run --prefix scripts/docs check

doc-open:
	@echo "$(GREEN)Opening documentation...$(NC)"
	$(CARGO) doc --workspace --no-deps --all-features --open

coverage:
	@echo "$(GREEN)Generating test coverage...$(NC)"
	$(CARGO) tarpaulin --workspace --all-features --timeout 300 --out Html

clean:
	@echo "$(YELLOW)Cleaning build artifacts...$(NC)"
	$(CARGO) clean

install-hooks:
	@echo "$(GREEN)Installing pre-commit hooks...$(NC)"
	pre-commit install
	pre-commit install --hook-type pre-push
	@echo "$(GREEN)Hooks installed successfully!$(NC)"

uninstall-hooks:
	@echo "$(YELLOW)Uninstalling pre-commit hooks...$(NC)"
	pre-commit uninstall
	pre-commit uninstall --hook-type pre-push

pre-commit-all:
	@echo "$(GREEN)Running pre-commit on all files...$(NC)"
	pre-commit run --all-files

update:
	@echo "$(GREEN)Updating dependencies...$(NC)"
	$(CARGO) update

outdated:
	@echo "$(GREEN)Checking for outdated dependencies...$(NC)"
	$(CARGO) outdated -R

bench:
	@echo "$(GREEN)Running benchmarks...$(NC)"
	$(CARGO) run -p elephant-benchmarks -- $(ARGS)

cargo-bench:
	@echo "$(GREEN)Running cargo bench targets...$(NC)"
	$(CARGO) bench --workspace

run-api:
	@echo "$(GREEN)Starting Elephant API server...$(NC)"
	$(CARGO) run -p elephant-api

run-pipeline:
	@echo "$(GREEN)Starting Elephant pipeline worker...$(NC)"
	$(CARGO) run -p elephant-pipeline-bin

xtask:
	@echo "$(GREEN)Running xtask...$(NC)"
	$(CARGO) xtask $(ARGS)

verify-version:
	@echo "$(GREEN)Verifying version...$(NC)"
	@VERSION=$$($(CARGO) metadata --no-deps --format-version 1 | python3 -c 'import json,sys; data=json.load(sys.stdin); print(next(pkg["version"] for pkg in data["packages"] if pkg["name"]=="elephant-types"))'); \
	TAG=$$(git describe --tags --exact-match 2>/dev/null | sed 's/^v//'); \
	if [ -z "$$TAG" ]; then \
		echo "$(YELLOW)No tag found on current commit$(NC)"; \
	elif [ "$$VERSION" != "$$TAG" ]; then \
		echo "$(RED)Version mismatch: Cargo.toml has $$VERSION but tag is $$TAG$(NC)"; \
		exit 1; \
	else \
		echo "$(GREEN)Version $$VERSION matches tag$(NC)"; \
	fi

release-preflight: fmt-check verify-surface verify-version-parity lint test-all audit doc
	@echo "$(GREEN)Release preflight complete!$(NC)"

publish-dry-run:
	@echo "$(GREEN)Running cargo package dry run...$(NC)"
	bash scripts/publish-dry-run.sh

help:
	@echo "Elephant CI/CD Makefile"
	@echo ""
	@echo "Development:"
	@echo "  $(GREEN)build$(NC)              - Build the workspace (debug)"
	@echo "  $(GREEN)release$(NC)            - Build optimized release version"
	@echo "  $(GREEN)run-api$(NC)            - Start the API server"
	@echo "  $(GREEN)run-pipeline$(NC)       - Start the pipeline worker"
	@echo "  $(GREEN)check$(NC)              - Quick compilation check"
	@echo ""
	@echo "Testing:"
	@echo "  $(GREEN)test$(NC)               - Run fast nextest lane"
	@echo "  $(GREEN)test-unit$(NC)          - Run workspace unit tests"
	@echo "  $(GREEN)test-all$(NC)           - Run full workspace all-features suite"
	@echo "  $(GREEN)test-e2e$(NC)           - Run deterministic E2E tests"
	@echo "  $(GREEN)test-e2e-model$(NC)     - Run the explicitly provisioned local-model regression"
	@echo "  $(GREEN)test-integration$(NC)   - Run ignored integration tests"
	@echo "  $(GREEN)test-scenarios$(NC)     - Run scenario tests"
	@echo "  $(GREEN)coverage$(NC)           - Generate test coverage report"
	@echo ""
	@echo "Code Quality:"
	@echo "  $(GREEN)lint$(NC)               - Run clippy linter"
	@echo "  $(GREEN)fmt$(NC)                - Fix code formatting"
	@echo "  $(GREEN)fmt-check$(NC)          - Check code formatting"
	@echo "  $(GREEN)audit$(NC)              - Run security audit (cargo-deny)"
	@echo "  $(GREEN)verify-surface$(NC)     - Verify active surface and legacy drift"
	@echo "  $(GREEN)verify-version-parity$(NC) - Verify workspace version parity"
	@echo ""
	@echo "CI/Release:"
	@echo "  $(GREEN)ci$(NC)                 - Run full CI pipeline"
	@echo "  $(GREEN)ci-smoke$(NC)           - Run faster smoke CI lane"
	@echo "  $(GREEN)release-preflight$(NC)  - Run release preflight checks"
	@echo "  $(GREEN)publish-dry-run$(NC)    - Run cargo package dry-run for publishable crates"
	@echo "  $(GREEN)install-hooks$(NC)      - Install git hooks"
	@echo "  $(GREEN)verify-version$(NC)     - Verify version matches git tag"
	@echo ""
	@echo "Documentation:"
	@echo "  $(GREEN)doc$(NC)                - Generate documentation"
	@echo "  $(GREEN)docs-check$(NC)         - Validate reader documentation"
	@echo "  $(GREEN)doc-open$(NC)           - Generate and open documentation"
	@echo ""
	@echo "Maintenance:"
	@echo "  $(GREEN)clean$(NC)              - Remove build artifacts"
	@echo "  $(GREEN)update$(NC)             - Update dependencies"
	@echo "  $(GREEN)outdated$(NC)           - Show outdated dependencies"
	@echo "  $(GREEN)help$(NC)               - Show this help message"
