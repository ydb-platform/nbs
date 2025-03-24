#!/bin/bash
set -euo pipefail

echo "🔍 Checking failed jobs in workflow run: $GITHUB_RUN_ID"

IFS=',' read -ra EXCLUDED <<< "$EXCLUDED_JOBS"
EXCLUDED+=("$SELF_JOB_NAME")

for ex in "${EXCLUDED[@]}"; do
    echo "🚫 Excluded job: $ex"
done

# Get job list from GitHub API
response=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com/repos/${REPO}/actions/runs/${GITHUB_RUN_ID}/jobs")

# Parse failed jobs
FAILED=()
JOB_COUNT=$(echo "$response" | jq '.jobs | length')
echo "📦 Found $JOB_COUNT jobs in this workflow run."

for row in $(echo "$response" | jq -r '.jobs[] | @base64'); do
    _jq() {
        echo "$row" | base64 --decode | jq -r "$1"
    }

    name=$(_jq '.name')
    conclusion=$(_jq '.conclusion')

    if [[ "$conclusion" == "failure" ]]; then
        skip=false
        for ex in "${EXCLUDED[@]}"; do
            if [[ "$name" == "$ex" ]]; then
                echo "⚠️  Excluding failed job: $name"
                skip=true
                break
            fi
        done
        if [[ "$skip" == false ]]; then
            echo "❌ Failed job: $name"
            FAILED+=("$name")
        fi
    fi
done

if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo ""
    echo "❗ The following jobs failed and are not excluded:"
    for job in "${FAILED[@]}"; do
        echo " - $job"
    done
    exit 1
else
    echo "✅ All required jobs passed."
fi
