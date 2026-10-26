#!/usr/bin/env bash
set -euo pipefail

run_step() {
    local name="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        echo "✓ $name"
    else
        echo "✗ $name"
        "$@"
        exit 1
    fi
}

run_step "cargo fmt" cargo fmt --all -- --check
run_step "typos" typos
run_step "cargo check" cargo check --workspace --all-targets --all-features
run_step "cargo clippy" cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::all -D dead_code -D unused
run_step "cargo test" cargo test --workspace --all-targets --all-features
run_step "cargo doc" cargo doc --workspace --no-deps

echo "All checks passed successfully!"
