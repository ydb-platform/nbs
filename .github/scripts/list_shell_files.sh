#!/usr/bin/env bash
set -euo pipefail

find .github -type f -name "*.sh" -print0

if [ -d .temporary ]; then
    find .temporary -type f -name "*.sh" -print0
fi
