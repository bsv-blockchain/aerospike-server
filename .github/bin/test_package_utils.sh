#!/usr/bin/env bash
set -euo pipefail

# Source the package utils
source "$(dirname "$0")/package_utils.sh"

echo "Testing get_codename_for_deb..."
# Test codename mapping
test_codename() {
    local input="$1"
    local expected="$2"
    local result
    if [ "$expected" = "ERROR" ]; then
        if ! result=$(get_codename_for_deb "$input" 2>&1); then
            echo "✓ $input -> failed as expected"
        else
            echo "✗ $input -> succeeded unexpectedly"
        fi
    else
        result=$(get_codename_for_deb "$input")
        if [ "$result" = "$expected" ]; then
            echo "✓ $input -> $result"
        else
            echo "✗ $input -> $result (expected $expected)"
        fi
    fi
}

test_codename "ubuntu20.04" "focal"
test_codename "ubuntu22.04" "jammy"
test_codename "ubuntu24.04" "noble"
test_codename "debian11" "bullseye"
test_codename "debian12" "bookworm"
test_codename "unknown" "ERROR" 