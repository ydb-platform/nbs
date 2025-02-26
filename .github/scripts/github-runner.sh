#!/usr/bin/env bash
set -x
set -e
set -o pipefail

# shellcheck disable=SC2317
function on_exit() {
    local exit_code=$?
    echo "Caught signal $exit_code, exiting..."
    df -h
    mount
    [ "$exit_code" -eq 1 ] && sleep 1800
    # shellcheck disable=SC2009
    ps auxf | grep -vE "]$"
    sudo ls -lsha "/home/${USER_TO_CREATE}/"
    sudo ls -lsha "/home/${USER_TO_CREATE}/.ssh/"
    [ -d "/home/${USER_TO_CREATE}/.ya" ] && {
        sudo ls -lsha "/home/${USER_TO_CREATE}/.ya"
        sudo du -h -d 1 "/home/${USER_TO_CREATE}/.ya"
    }
    if [ -n "$FILENAME" ] && [ -f "/home/${USER_TO_CREATE}/${FILENAME}" ]; then
        sudo rm -f "/home/${USER_TO_CREATE}/${FILENAME}"
    fi
    sudo rm -rf "/home/${USER_TO_CREATE}/.aws" /root/.aws/

    sudo rm -rf "/home/${USER_TO_CREATE}/.aws" /root/.aws/
    sudo rm -rf /var/lib/apt/lists/*
    sudo cloud-init clean --logs
    sync
    exit "$exit_code"
}
trap on_exit EXIT

# Download github runner
sudo mkdir -p /actions-runner && cd /actions-runner || exit
sudo curl -o runner.tar.gz -L "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
sudo tar xzf ./runner.tar.gz
# we do not have v6 connectivity on vms
echo 'Acquire::ForceIPv4 "true";' | sudo tee /etc/apt/apt.conf.d/99force-ipv4
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo 'deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu jammy stable' | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get install -y --no-install-recommends \
             git wget gnupg lsb-release curl tzdata python3-dev \
             python3-pip libidn11-dev file s3cmd qemu-kvm qemu-utils \
             dpkg-dev docker-ce docker-ce-cli containerd.io \
             docker-buildx-plugin docker-compose-plugin jq \
             aria2 jq tree tmux atop awscli iftop htop \
             pixz pigz pbzip2 xz-utils gdb unzip
cat << EOF > /tmp/requirements.txt
pytest
pyinstaller
pytest-timeout
pytest-xdist
setproctitle
six
pyyaml
packaging
cryptography
grpcio
grpcio-tools
PyHamcrest
tornado
xmltodict
pyarrow
boto3
psutil
PyGithub==2.5.0
cryptography
protobuf
pyOpenSSL
packaging
rapidgzip
typing-extensions
nebius==0.2.6
EOF
sudo pip3 install -r /tmp/requirements.txt
curl -L "https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}-linux-${OS_ARCH}.tar.xz" | sudo tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner "ccache-${CCACHE_VERSION}-linux-${OS_ARCH}/ccache"
sudo apt-get remove -y unattended-upgrades

sudo adduser --gecos "" --disabled-password --shell /bin/bash "${USER_TO_CREATE}"
set +x
sudo usermod --password "${PASSWORD_HASH//$/\\$}" "${USER_TO_CREATE}"
set -x
sudo sed -i -e 's/\\\$/$/g' /etc/shadow
sudo usermod -a -G kvm "${USER_TO_CREATE}"
sudo usermod -a -G docker "${USER_TO_CREATE}"
sudo echo "${USER_TO_CREATE} ALL=(ALL) NOPASSWD:ALL" | sudo tee "/etc/sudoers.d/99-${USER_TO_CREATE}" > /dev/null
sudo chmod 0440 "/etc/sudoers.d/99-${USER_TO_CREATE}"

# increase the total number of aio requests to run more tests in parallel, default is 65536
echo "fs.aio-max-nr=1048576" >> /etc/sysctl.conf

if [ -n "$GITHUB_TOKEN" ] && [ -n "$ORG" ] && [ -n "$TEAM" ]; then
    export LOGINS_FILE
    export KEYS_FILE
    LOGINS_FILE=$(mktemp)
    KEYS_FILE=$(mktemp)

    # Get team members
    curl -s -L \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer $GITHUB_TOKEN" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "https://api.github.com/orgs/${ORG}/teams/${TEAM}/members?per_page=100&page=1" | jq -r '.[].login' | tee -a "$LOGINS_FILE"

    # Get members ssh keys
    while read -r login; do
        curl -s -L \
            -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer $GITHUB_TOKEN" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            "https://api.github.com/users/${login}/keys" | jq -r '.[].key' | while read -r key; do
            echo "$key $login" | tee -a "$KEYS_FILE"
        done
    done < "$LOGINS_FILE"
fi

if [ -f "$KEYS_FILE" ]; then
    sudo mkdir -p "/home/${USER_TO_CREATE}/.ssh"
    sudo chown -R "${USER_TO_CREATE}:${USER_TO_CREATE}" "/home/${USER_TO_CREATE}/.ssh"
    sudo chmod 0700 "/home/${USER_TO_CREATE}/.ssh"
    sudo cp "${KEYS_FILE}" "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
    sudo chown "${USER_TO_CREATE}:${USER_TO_CREATE}" "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
    sudo chmod 0600 "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
    sudo cat "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
fi

# add .ya cache
sudo ls -lsha "/home/${USER_TO_CREATE}/"
df -h
if [ -n "${YA_ARCHIVE_URL}" ]; then
    FILENAME="${YA_ARCHIVE_URL##*/}"
    EXTENSION="${FILENAME##*.}"
    PROTOCOL="${YA_ARCHIVE_URL%%://*}"
    case "$EXTENSION" in
        gz)
            COMPRESS_ARGS=rapidgzip
            ;;
        xz)
            COMPRESS_ARGS='pixz'
            ;;
        bz2)
            COMPRESS_ARGS=pbzip2
            ;;
        zst | zstd)
            COMPRESS_ARGS=zstd
            ;;
        *)
            echo "Unsupported archive extension: $EXTENSION"
            exit 1
            ;;
    esac

    # wget -nv -O - "${YA_ARCHIVE_URL}" | sudo -E -H -u github time tar -S -I "$COMPRESS_ARGS" -C "/home/${USER_TO_CREATE}" --strip-components=2 -x -f -
    case "$PROTOCOL" in
        http | https)
            sudo -E -H -u "$USER_TO_CREATE" time aria2c -x 8 -d "/home/${USER_TO_CREATE}" "${YA_ARCHIVE_URL}" -d "/home/${USER_TO_CREATE}" -o "${FILENAME}"
            ;;
        s3)
            if [ -z "$AWS_ACCESS_KEY_ID" ] && [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
                echo "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set"
                exit 1
            fi
            sudo -E -H -u "$USER_TO_CREATE" mkdir -p "/home/$USER_TO_CREATE/.aws"
            cat << EOF | sudo -E -H -u "$USER_TO_CREATE" tee "/home/$USER_TO_CREATE/.aws/config"
[default]
aws_access_key_id = $AWS_ACCESS_KEY_ID
aws_secret_access_key = $AWS_SECRET_ACCESS_KEY
EOF
            cat << EOF | sudo -E -H -u "$USER_TO_CREATE" tee "/home/$USER_TO_CREATE/.aws/credentials"
[default]
region = eu-north1
endpoint_url=https://storage.ai.nebius.cloud/
s3 =
    max_concurrent_requests = 32
    multipart_chunksize = 32MB
    max_queue_size = 10240
EOF
            sudo -E -H -u github time aws s3 cp "${YA_ARCHIVE_URL}" "/home/${USER_TO_CREATE}/${FILENAME}"
            ;;
        *)
            echo "Unsupported protocol: $PROTOCOL"
            exit 1
            ;;
    esac
    sudo -E -H -u "$USER_TO_CREATE" time tar -S -I "$COMPRESS_ARGS" -C "/home/${USER_TO_CREATE}" --strip-components=2 -x -f "/home/${USER_TO_CREATE}/${FILENAME}"
fi

sync

# some healthchecks
healthchecks_exit_code=0
sudo test -s "/home/${USER_TO_CREATE}/.ssh/authorized_keys" || {
    echo "Authorized keys is empty"
    sudo ls -lsha "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
    healthchecks_exit_code=1
}
sudo grep 'github:\$' /etc/shadow > /dev/null 2> /dev/null || {
    echo "User github either do not exist or has wrong hash"
    healthchecks_exit_code=1
}
exit $healthchecks_exit_code
