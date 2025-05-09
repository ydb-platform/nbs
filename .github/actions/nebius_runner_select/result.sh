#!/usr/bin/env bash
LABELS=${INSTANCE_LABELS/idle=true/idle=false}
LABELS=$(echo "$LABELS" | sed -e "s/run=[^,]*//g" -e 's/,,/,/g' -e "s/$/,run=${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}/g")
if [ "${PR_NUMBER}" == "no" ]; then
    LABELS=$(echo "$LABELS" | sed -e 's/pr=[^,]*//g' -e 's/,,/,/g')
else
    LABELS=$(echo "$LABELS" | sed -e "s/pr=[^,]*//g" -e 's/,,/,/g' -e "s/$/,pr=${PR_NUMBER}/g")
fi
LABELS=${LABELS/,,/,}
nebius compute instance get --id "$INSTANCE_ID" --format json >instance.json

OLD_INSTANCE_NAME=$(jq -r '.metadata.name' instance.json)

LABEL=$(jq -r '.metadata.labels."runner-label"' instance.json)
RUNNER_FLAVOR=$(jq -r '.metadata.labels."runner-flavor"' instance.json)
EXTERNAL_IPV4=$(jq -r '.status.network_interfaces[0].public_ip_address.address' instance.json | sed -e 's/\/32//g')
LOCAL_IPV4=$(jq -r '.status.network_interfaces[0].ip_address.address' instance.json | sed -e 's/\/32//g')
PRESET=$(jq -r '.spec.resources.preset' instance.json)
DATE=$(date +%s)
if [[ $OLD_INSTANCE_NAME == running-* ]]; then
    OLD_INSTANCE_NAME="${RUNNER_FLAVOR}-${GITHUB_REPOSITORY_OWNER}-${REPOSITORY_NAME}-$DATE"
fi

INSTANCE_NAME=$OLD_INSTANCE_NAME
if [ -n "${NEW_INSTANCE_NAME}" ]; then
    echo "New instance name: $NEW_INSTANCE_NAME"
    INSTANCE_NAME="${NEW_INSTANCE_NAME}"
    echo "New instance name: $INSTANCE_NAME"
    nebius compute instance update --id "$INSTANCE_ID" --name "$NEW_INSTANCE_NAME" --labels "$LABELS"
else
    echo "Instance name not changed"
    nebius compute instance update --id "$INSTANCE_ID" --labels "$LABELS"
fi

{
    echo "LABEL=${LABEL}"
    echo "RUNNER_FLAVOR=${RUNNER_FLAVOR}"
    echo "INSTANCE_ID=${INSTANCE_ID}"
    echo "INSTANCE_NAME=${INSTANCE_NAME}"
    echo "RUNNER_IPV4=${EXTERNAL_IPV4}"
    echo "RUNNER_LOCAL_IPV4=${LOCAL_IPV4}"
    echo "OLD_INSTANCE_NAME=${OLD_INSTANCE_NAME}"
    echo "VM_PRESET=${PRESET}"
} | tee -a "$GITHUB_OUTPUT"
