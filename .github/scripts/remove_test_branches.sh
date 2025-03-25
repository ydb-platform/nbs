#!/bin/bash

set -euo pipefail

# Ensure we're running against the correct repo
EXPECTED_REPO="librarian-test/nbs"
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)

if [[ "$REPO" != "$EXPECTED_REPO" ]]; then
    echo "❌ Error: This script is only allowed to run on '$EXPECTED_REPO'."
    echo "Current repository: '$REPO'"
    exit 1
fi

MAIN_BRANCH="main"

echo "✅ Cleaning up test branches in repository: $REPO"

# Get all remote branches except main
branches=$(git ls-remote --heads origin | awk '{print $2}' | sed 's#refs/heads/##' | grep -v "^${MAIN_BRANCH}$")

for branch in $branches; do
    echo "🔍 Processing branch: $branch"

    # Check for associated PR
    pr_json=$(gh pr --repo "$REPO" list --head "$branch" --state open --json number,state -q '.[]?')

    if [[ -n "$pr_json" ]]; then
        pr_number=$(echo "$pr_json" | jq -r '.number')
        echo "  📌 Found PR #$pr_number"

        # Check if checks are running
        checks_running=$(gh pr --repo "$REPO" checks "$pr_number" --json state -q '[.[] | select(.state=="IN_PROGRESS" or .state=="QUEUED")] | length')

        if [[ "$checks_running" -eq 0 ]]; then
            echo "  ✅ No checks running. Closing PR and deleting branch..."
            gh pr --repo "$REPO" close "$pr_number" --delete-branch
        else
            echo "  ⏳ Checks are still running. Skipping."
        fi
    else
        echo "  🧹 No PR found. Deleting remote branch..."
        git push origin --delete "$branch"
    fi
done
