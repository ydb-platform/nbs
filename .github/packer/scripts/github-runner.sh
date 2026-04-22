#!/usr/bin/env bash
set -x
set -e
set -o pipefail

# shellcheck disable=SC2317,SC2329
function on_exit() {
    local exit_code=$?
    echo "Caught signal $exit_code, exiting..."
    df -h || true
    mount || true
    # TODO: sleep for more if it will be build like in ycloud
    [ "$exit_code" -eq 1 ] && sleep 1
    # shellcheck disable=SC2009
    ps auxf | grep -vE "]$" || true
    ls -lsha "/home/${USER_TO_CREATE}/" || true
    ls -lsha "/home/${USER_TO_CREATE}/.ssh/" || true
    if ls -lsha "/home/${USER_TO_CREATE}/.ya"; then
        du -h -d 1 "/home/${USER_TO_CREATE}/.ya"
    fi
    rm -f "/home/${USER_TO_CREATE}/${FILENAME}" || true
    rm -rf "/home/${USER_TO_CREATE}/.aws" /root/.aws/ || true
    rm -rf /var/lib/apt/lists/* || true
    cloud-init clean --logs || true
    sync
    exit "$exit_code"
}
trap on_exit EXIT

# Download github runner
mkdir -p /actions-runner && cd /actions-runner || exit
curl -o runner.tar.gz -L "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
tar xzf ./runner.tar.gz

install -m 0755 -d /etc/apt/keyrings
# we do not have v6 connectivity on vms
echo 'Acquire::ForceIPv4 "true";' | tee /etc/apt/apt.conf.d/99force-ipv4
# set the default timeout to 10 minutes
echo "Dpkg::Lock::Timeout=600;" | tee /etc/apt/apt.conf.d/99dpkg-lock-timeout
# noninteractive mode
# we need to retry it because sometimes apt-get can be locked by cloud-init or unattended-upgrades, and we want to avoid race conditions in that case
for i in {1..6}; do
    if echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections; then
        break
    else
        echo "Failed to set debconf frontend, retrying... ($i/6)"
        sleep 10
    fi
done

# docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo 'deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu jammy stable' | tee /etc/apt/sources.list.d/docker.list > /dev/null

# github cli
wget -nv -O/etc/apt/keyrings/githubcli-archive-keyring.gpg https://cli.github.com/packages/githubcli-archive-keyring.gpg
chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null

# nebius cli
# nosemgrep: bash.curl.security.curl-pipe-bash.curl-pipe-bash
curl -sSL https://storage.eu-north1.nebius.cloud/cli/install.sh | bash

apt-get update
apt-get -y upgrade

# linux-image-6.2.0-39-generic is important because default kernel doesn't allow
# `nvme --id-ctrl` without CAP_SYS_ADMIN or root privilege
LINUX_VER=6.2.0-39-generic

LINUX_PKGS="linux-image-${LINUX_VER} \
    linux-modules-${LINUX_VER} \
    linux-modules-extra-${LINUX_VER} \
    linux-tools-${LINUX_VER} \
    linux-tools-common"

apt-get install -y --no-install-recommends \
    ${LINUX_PKGS} \
    git wget gnupg lsb-release curl tzdata \
    libidn11-dev libaio1 libaio-dev \
    file s3cmd qemu-kvm qemu-utils \
    python3-dev python3-pip \
    dpkg-dev docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin \
    jq tree tmux atop iftop htop unzip \
    pixz pigz pbzip2 xz-utils gdb zram-tools \
    nvme-cli

# remove it for now, after migration we will return it back
#apt-get remove -y unattended-upgrades
#apt-get purge -y unattended-upgrades

pip3 install -r /tmp/packer/requirements.txt

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf awscliv2.zip

adduser --gecos "" --disabled-password --shell /bin/bash "${USER_TO_CREATE}"
set +x
usermod --password "${PASSWORD_HASH//$/\\$}" "${USER_TO_CREATE}"
set -x
sed -i -e 's/\\\$/$/g' /etc/shadow
usermod -a -G kvm "${USER_TO_CREATE}"
usermod -a -G docker "${USER_TO_CREATE}"
echo "${USER_TO_CREATE} ALL=(ALL) NOPASSWD:ALL" | tee "/etc/sudoers.d/99-${USER_TO_CREATE}" > /dev/null
chmod 0440 "/etc/sudoers.d/99-${USER_TO_CREATE}"

# allow coredumps
sudo tee -a /etc/security/limits.conf << EOF
* soft core unlimited
* hard core unlimited
EOF

echo "Defaults rlimit_core=default" | sudo tee /etc/sudoers.d/98-rlimit
sudo chmod 0440 /etc/sudoers.d/98-rlimit

# increase the total number of aio requests to run more tests in parallel, default is 65536
echo "fs.aio-max-nr=1048576" >> /etc/sysctl.conf
echo "vm.swappiness=1" >> /etc/sysctl.conf
echo "SIZE=102400" | sudo tee -a /etc/default/zramswap
echo "PRIORITY=2" | sudo tee -a /etc/default/zramswap
echo "ALGO=lz4" | sudo tee -a /etc/default/zramswap

# Set atop logging interval to 30 seconds
if grep -q '^LOGINTERVAL=' /etc/default/atop; then
    # Update existing LOGINTERVAL line
    sed -i 's/^LOGINTERVAL=.*/LOGINTERVAL=30/' /etc/default/atop
else
    # Add LOGINTERVAL line if not present
    echo "LOGINTERVAL=30" >> /etc/default/atop
fi

# remove auto-logout timeout
[ -f /etc/profile.d/tmout.sh ] && rm /etc/profile.d/tmout.sh

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
    mkdir -p "/home/${USER_TO_CREATE}/.ssh"
    chown -R "${USER_TO_CREATE}:${USER_TO_CREATE}" "/home/${USER_TO_CREATE}/.ssh"
    chmod 0700 "/home/${USER_TO_CREATE}/.ssh"
    cp "${KEYS_FILE}" "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
    chown "${USER_TO_CREATE}:${USER_TO_CREATE}" "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
    chmod 0600 "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
    cat "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
fi

sync

# some healthchecks
healthchecks_exit_code=0
test -s "/home/${USER_TO_CREATE}/.ssh/authorized_keys" || {
    echo "Authorized keys is empty"
    ls -lsha "/home/${USER_TO_CREATE}/.ssh/authorized_keys"
    healthchecks_exit_code=1
}
grep 'github:\$' /etc/shadow > /dev/null 2> /dev/null || {
    echo "User github either do not exist or has wrong hash"
    healthchecks_exit_code=1
}
exit $healthchecks_exit_code
