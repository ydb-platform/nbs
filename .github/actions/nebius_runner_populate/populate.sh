#!/usr/bin/env bash
set -x
nebius compute instance list --parent-id "${PARENT_ID}" --format json | jq --arg owner "${OWNER}" --arg repo "${REPO}" --arg flavor "${FLAVOR}" -r '
.items[] |
select(.metadata.labels.repo == $repo) |
select(.metadata.labels.owner == $owner) |
select(.status.state == "RUNNING") |
select(.metadata.labels."runner-flavor" == $flavor) |
"\(.metadata.created_at) \(.metadata.id) \(.metadata.name) \(.status.state) \(.metadata.labels."runner-label") \((.metadata.labels | to_entries | map("\(.key)=\(.value)") | join(",")))"' >instances.list || touch instances.list

RUNNING_VMS_COUNT=$(wc -l instances.list | awk '{print $1}')
VMS_TO_REMOVE='[]'
echo "$VMS_TO_REMOVE" >vms_to_remove.json
echo 0 >vms_count_to_remove
VMS_COUNT_TO_REMOVE=0
DATE=$(date +%s)
while read -r vm_creation_date vm_id name state label labels; do
    echo "Checking $vm_id"
    vm_creation_date_ts=$(date -u -d "$vm_creation_date" +%s)
    echo "VM $vm_id created at $vm_creation_date ($vm_creation_date_ts)"
    echo "VM $vm_id is $((DATE - vm_creation_date_ts)) seconds old"
    if [ $((DATE - vm_creation_date_ts)) -gt "$VMS_OLDER_THAN" ]; then
        echo "VM $vm_id is older than $VMS_OLDER_THAN seconds"
        echo "Checking if vm is idle in GH"
        gh api -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" "/repos/$OWNER/$REPO/actions/runners?per_page=100" | jq -r --arg vm_id "$vm_id" '.runners[] | select(.name == $vm_id)' | tee -a "gh_vm_${vm_id}_status.json"
        gh_vm_status=$(jq -r '.busy' "gh_vm_${vm_id}_status.json")
        if [ "$gh_vm_status" == "false" ]; then
            echo "VM $vm_id is idle in GH"
        else
            echo "VM $vm_id is not idle in GH"
            continue
        fi
        VMS_TO_REMOVE=$(echo "$VMS_TO_REMOVE" | jq -c --arg vm_id "$vm_id" '. + [$vm_id]')
        VMS_COUNT_TO_REMOVE=$((VMS_COUNT_TO_REMOVE + 1))
        echo "$VMS_TO_REMOVE" >vms_to_remove.json
    fi
done <instances.list
echo "VMS_TO_REMOVE=$(cat vms_to_remove.json)"
echo "VMS_COUNT_TO_REMOVE=$VMS_COUNT_TO_REMOVE"
echo "MAX_VMS_TO_CREATE=$MAX_VMS_TO_CREATE"
echo "RUNNING_VMS_COUNT=$RUNNING_VMS_COUNT"
NUMBER_VMS_TO_CREATE=$((MAX_VMS_TO_CREATE - RUNNING_VMS_COUNT + VMS_COUNT_TO_REMOVE))
echo "NUMBER_VMS_TO_CREATE=$NUMBER_VMS_TO_CREATE"
if [ $NUMBER_VMS_TO_CREATE -le 0 ]; then
    echo "No VMs to create"
    echo "[]" >vms_to_create.json
else
    for i in $(seq 1 $NUMBER_VMS_TO_CREATE); do
        echo "${FLAVOR}-${OWNER}-${REPO}-$DATE-$i"
    done | jq -R -s -c 'split("\n") | map(select(. != ""))' >vms_to_create.json
fi
{
    echo "RUNNING_VMS_COUNT=${RUNNING_VMS_COUNT}"
    echo "VMS_TO_CREATE=$(cat vms_to_create.json)"
    echo "VMS_TO_REMOVE=$(cat vms_to_remove.json)"
    echo "DATE=$DATE"
} | tee -a "$GITHUB_OUTPUT"
