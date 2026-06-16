#!/usr/bin/env bash
# Template placeholders are rendered by nebius_manage_vm.py before execution.
# shellcheck disable=SC1083
UPDATE_RUNNER={override_existing_runner}
RUNNER_VERSION={version}
RUNNER_SHA256_X64={sha256_x64}
RUNNER_SHA256_ARM64={sha256_arm64}
RUNNER_LABELS={label}
REPO_URL={repo_url}
RUNNER_USER={runner_user}
RUNNER_REGISTRATION_TOKEN={token}

set -x

echo "fixing /etc/hosts"
echo "::1 localhost" | tee -a /etc/hosts
grep localhost /etc/hosts

mkdir -p /coredumps
chmod 1777 /coredumps
grep -qF "kernel.core_pattern=/coredumps/core.%e.%u.%b.%p.%t" /etc/sysctl.conf || echo "kernel.core_pattern=/coredumps/core.%e.%u.%b.%p.%t" >> /etc/sysctl.conf
grep -qF "kernel.core_uses_pid=1" /etc/sysctl.conf || echo "kernel.core_uses_pid=1" >> /etc/sysctl.conf
grep -qF "fs.suid_dumpable=0" /etc/sysctl.conf || echo "fs.suid_dumpable=0" >> /etc/sysctl.conf
grep -qF "* soft memlock unlimited" /etc/security/limits.conf || echo "* soft memlock unlimited" >> /etc/security/limits.conf
grep -qF "* hard memlock unlimited" /etc/security/limits.conf || echo "* hard memlock unlimited" >> /etc/security/limits.conf
sysctl -p || true

RUNNER_HOME=$(getent passwd "$RUNNER_USER" | cut -d: -f6)
if [ -z "$RUNNER_HOME" ]; then
    echo "Runner user does not exist: $RUNNER_USER"
    exit 1
fi

if [ ! -x /usr/local/bin/actions-runner-collect-system-logs.sh ]; then
    cat > /usr/local/bin/actions-runner-collect-system-logs.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 0 ]; then
    echo "usage: $0" >&2
    exit 2
fi

LOG_DIR=/home/github/tmp_test/logs
SYSTEM_LOGS_DIR=$LOG_DIR/system_logs
KERN_LOG_PATH=$LOG_DIR/kern.log
TMP_TEST_DIR=/home/github/tmp_test

RUNNER_UID=${{SUDO_UID:-}}
RUNNER_GID=${{SUDO_GID:-}}
if [ -z "$RUNNER_UID" ] || [ -z "$RUNNER_GID" ]; then
    RUNNER_UID=$(id -u github)
    RUNNER_GID=$(id -g github)
fi

TMP_DIR=$(mktemp -d /tmp/actions-runner-system-logs.XXXXXX)
TMP_SYSTEM_LOGS_DIR=$TMP_DIR/system_logs
TMP_KERN_LOG_PATH=$TMP_DIR/kern.log

cleanup() {{
    rm -rf "$TMP_DIR"
}}
trap cleanup EXIT

mkdir -p "$TMP_SYSTEM_LOGS_DIR"

if dmesg -T > "$TMP_SYSTEM_LOGS_DIR/dmesg.log" 2> /dev/null; then
    chmod 0644 "$TMP_SYSTEM_LOGS_DIR/dmesg.log"
fi

if [ -r /var/log/kern.log ]; then
    install -m 0644 /var/log/kern.log "$TMP_KERN_LOG_PATH"
fi

if [ -r /var/log/syslog ]; then
    install -m 0644 /var/log/syslog "$TMP_SYSTEM_LOGS_DIR/syslog.log"
fi

for atop_log in /var/log/atop/atop_*; do
    if [ -r "$atop_log" ]; then
        install -m 0644 "$atop_log" "$TMP_SYSTEM_LOGS_DIR/$(basename "$atop_log")"
    fi
done

for dir in "$TMP_TEST_DIR" "$LOG_DIR"; do
    if [ -L "$dir" ] || {{ [ -e "$dir" ] && [ ! -d "$dir" ]; }}; then
        rm -f "$dir"
    fi
    mkdir -p "$dir"
    if [ -L "$dir" ] || [ ! -d "$dir" ]; then
        echo "Refusing to publish system logs through unsafe path: $dir" >&2
        exit 1
    fi
    chown "$RUNNER_UID:$RUNNER_GID" "$dir"
    chmod 0755 "$dir"
done

rm -rf "$SYSTEM_LOGS_DIR"
rm -f "$KERN_LOG_PATH"

find "$TMP_SYSTEM_LOGS_DIR" -type d -exec chmod 0755 {{}} +
find "$TMP_SYSTEM_LOGS_DIR" -type f -exec chmod 0644 {{}} +
chown -R "$RUNNER_UID:$RUNNER_GID" "$TMP_SYSTEM_LOGS_DIR"
mv -T "$TMP_SYSTEM_LOGS_DIR" "$SYSTEM_LOGS_DIR"

if [ -e "$TMP_KERN_LOG_PATH" ]; then
    chown "$RUNNER_UID:$RUNNER_GID" "$TMP_KERN_LOG_PATH"
    chmod 0644 "$TMP_KERN_LOG_PATH"
    mv -T "$TMP_KERN_LOG_PATH" "$KERN_LOG_PATH"
fi

EOF
    chmod 0755 /usr/local/bin/actions-runner-collect-system-logs.sh
fi

cat > "/etc/sudoers.d/99-$RUNNER_USER" << EOF
Cmnd_Alias GITHUB_RUNNER_LOGS = /usr/local/bin/actions-runner-collect-system-logs.sh
$RUNNER_USER ALL=(root) NOPASSWD: GITHUB_RUNNER_LOGS
EOF
chmod 0440 "/etc/sudoers.d/99-$RUNNER_USER"

if [ "$UPDATE_RUNNER" = "true" ]; then
    mkdir -p /actions-runner
    cd /actions-runner || exit
    case "$(uname -m)" in
        aarch64)
            ARCH="arm64"
            EXPECTED_SHA256="$RUNNER_SHA256_ARM64"
            ;;
        amd64 | x86_64)
            ARCH="x64"
            EXPECTED_SHA256="$RUNNER_SHA256_X64"
            ;;
        *)
            echo "Unsupported architecture: $(uname -m)"
            exit 1
            ;;
    esac
    if [ -z "$EXPECTED_SHA256" ]; then
        echo "Missing expected SHA-256 for runner architecture: $ARCH"
        exit 1
    fi
    FILENAME="runner-v$RUNNER_VERSION.tar.gz"
    exit_code=1
    i=0
    url="https://github.com/actions/runner/releases/download/v$RUNNER_VERSION/actions-runner-linux-$ARCH-$RUNNER_VERSION.tar.gz"
    until [ "$exit_code" -eq 0 ] || [ "$i" -gt 3 ]; do
        [ -f "$FILENAME" ] || curl --connect-timeout 5 -fsSL "$url" -o "$FILENAME"
        exit_code=$?
        if [ "$exit_code" -eq 0 ]; then
            echo "$EXPECTED_SHA256  $FILENAME" | sha256sum -c -
            exit_code=$?
        fi
        i=$((i + 1))
        [ "$exit_code" -eq 0 ] || rm -f "$FILENAME"
        echo "$(date) [$i] runner download/checksum exited with code $exit_code"
    done
    if [ "$exit_code" -ne 0 ]; then
        echo "Failed to download and verify GitHub runner $RUNNER_VERSION for $ARCH"
        exit 1
    fi
    # there was exit 0 but idk why
    tar xzf "./$FILENAME" || exit 1
elif [ -d /actions-runner ]; then
    echo "Runner already installed and doesn't require update, skipping installation"
    cd /actions-runner || exit
else
    echo "Runner is not installed and UPDATE_RUNNER is not true"
    exit 1
fi

chown -R "$RUNNER_USER:$RUNNER_USER" /actions-runner

# trying to catch registration error
exit_code=1
i=0
until [ "$exit_code" -eq 0 ] || [ "$i" -gt 3 ]; do
    echo ./config.sh --labels "$RUNNER_LABELS" --url "$REPO_URL" --token XXX --unattended
    set +x
    timeout 60 env HOME="$RUNNER_HOME" USER="$RUNNER_USER" LOGNAME="$RUNNER_USER" runuser -u "$RUNNER_USER" -- ./config.sh --labels "$RUNNER_LABELS" --url "$REPO_URL" --token "$RUNNER_REGISTRATION_TOKEN" --unattended
    set -x
    exit_code=$?
    i=$((i + 1))
    echo "$(date) [$i] config.sh exited (or timed-out) with code $exit_code"
    if [ "$exit_code" -ne 0 ]; then
        find /actions-runner -name '*.log' -print -exec cat '{{}}' ';'
    fi
done

if [ "$exit_code" -ne 0 ]; then
    echo "Failed to configure GitHub runner after $i attempt(s)"
    exit 1
fi

touch /actions-runner/.env
sed -i '/^ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/d' /actions-runner/.env
echo "ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/usr/local/bin/actions-runner-job-completed-cleanup.sh" >> /actions-runner/.env
chown -R "$RUNNER_USER:$RUNNER_USER" /actions-runner

# true to skip the error and to boot vm correctly
sed -i \
    '/^\[Install\]/i \
OOMPolicy=continue \
OOMScoreAdjust=-900 \
Delegate=yes \
TasksMax=infinity \
LimitMEMLOCK=infinity \
Restart=on-failure \
RestartSec=5s \
Slice=actions-runner.slice' \
    ./bin/actions.runner.service.template
./svc.sh install "$RUNNER_USER" || true
./svc.sh start || true
