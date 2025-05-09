#!/usr/bin/env bash
while true; do
    # clear old lists
    truncate -s0 instances.list
    truncate -s0 instances_not_idle.list

    nebius compute instance list --parent-id "${PARENT_ID}" --format json | jq --arg owner "${OWNER}" --arg repo "${REPO}" --arg flavor "${FLAVOR}" -r '
    .items[] |
    select(.metadata.labels.repo == $repo) |
    select(.metadata.labels.owner == $owner) |
    select(.metadata.labels.idle == "true") |
    select(.status.state == "RUNNING") |
    select(.metadata.labels."runner-flavor" == $flavor) |
    "\(.metadata.id) \(.metadata.labels."runner-label") \((.metadata.labels | to_entries | map("\(.key)=\(.value)") | join(",")))"' |
        tee -a instances.list || touch instances.list

    if [ -s instances.list ]; then
        head -n 1 instances.list | while read -r instance_id github_label instance_labels; do
            echo "Selected id: $instance_id"
            echo "Selected github label: $github_label"
            echo "Selected instance_labels: $instance_labels"
            {
                echo "INSTANCE_ID=$instance_id"
                echo "LABEL=$github_label"
                echo "INSTANCE_LABELS=$instance_labels"
            } | tee -a "$GITHUB_OUTPUT"
        done
        exit 0
    fi

    echo "No available instances found, checking idle=false ones..."

    nebius compute instance list --parent-id "${PARENT_ID}" --format json | jq --arg owner "${OWNER}" --arg repo "${REPO}" --arg flavor "${FLAVOR}" -r '
    .items[] |
    select(.metadata.labels.repo == $repo) |
    select(.metadata.labels.owner == $owner) |
    select(.metadata.labels.idle == "false") |
    select(.status.state == "RUNNING") |
    select(.metadata.labels."runner-flavor" == $flavor) |
    "\(.metadata.id) \(.metadata.labels."runner-label") \((.metadata.labels | to_entries | map("\(.key)=\(.value)") | join(",")))"' |
        tee -a instances_not_idle.list || touch instances_not_idle.list

    if [ -s instances_not_idle.list ]; then
        echo "Found instances with idle=false, checking availability..."
        success=0
        while read -r instance_id github_label instance_labels; do
            echo "Checking id: $instance_id"
            run_id=$(echo "$instance_labels" | sed -e 's/.*run=\([^,]*\).*/\1/g' | cut -d'-' -f1)
            echo "Checking run id: $run_id"
            run_status=$(gh run view "$run_id" --json status | jq -r .status)
            echo "Run status: $run_status"
            if [ "$run_status" == "completed" ]; then
                echo "Run $run_id is completed, setting instance $instance_id to idle=true"
                instance_labels=${instance_labels/idle=false/idle=true}
                nebius compute instance update --id "$instance_id" --labels "$instance_labels"
                echo "Selected id: $instance_id"
                echo "Selected github label: $github_label"
                echo "Selected instance_labels: $instance_labels"
                {
                    echo "INSTANCE_ID=$instance_id"
                    echo "LABEL=$github_label"
                    echo "INSTANCE_LABELS=$instance_labels"
                } | tee -a "$GITHUB_OUTPUT"
                success=1
                break
            fi
        done <instances_not_idle.list

        if [ $success -eq 1 ]; then
            exit 0
        else
            echo "No available idle=false instances found"
        fi
    else
        echo "No instances with idle=false found"
    fi

    # optional wait before next loop
    echo "Retrying in 10 seconds..."
    sleep 10
done
