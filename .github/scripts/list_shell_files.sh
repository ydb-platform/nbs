#!/usr/bin/env bash
set -euo pipefail

find .github -type f -print0 \
    | while IFS= read -r -d '' file; do
        case "$file" in
            *.sh | *.bash | *.bats)
                printf '%s\0' "$file"
                continue
                ;;
        esac
        read -r first_line < "$file" || true
        case "$first_line" in
            '#!'*sh* | '#!'*bash* | '#!'*dash* | '#!'*ksh* | '#!'*zsh*)
                printf '%s\0' "$file"
                ;;
        esac
    done

if [ -d .temporary ]; then
    find .temporary -type f -name "*.sh" -print0
fi
