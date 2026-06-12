#!/usr/bin/env bash
# Template placeholders are rendered by nebius_manage_vm.py before execution.
# shellcheck disable=SC1083
UPDATE_RUNNER={override_existing_runner}
RUNNER_VERSION={version}
RUNNER_SHA256_X64={sha256_x64}
RUNNER_SHA256_ARM64={sha256_arm64}
RUNNER_LABELS={label}
REPO_URL={repo_url}
RUNNER_REGISTRATION_TOKEN={token}

set -x

echo "fixing /etc/hosts"
echo "::1 localhost" | tee -a /etc/hosts
grep localhost /etc/hosts

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
export RUNNER_ALLOW_RUNASROOT=1

# trying to catch registration error
exit_code=1
i=0
until [ "$exit_code" -eq 0 ] || [ "$i" -gt 3 ]; do
    echo ./config.sh --labels "$RUNNER_LABELS" --url "$REPO_URL" --token XXX --unattended
    set +x
    timeout 60 ./config.sh --labels "$RUNNER_LABELS" --url "$REPO_URL" --token "$RUNNER_REGISTRATION_TOKEN" --unattended
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

# true to skip the error and to boot vm correctly
sed -i \
    '/^\[Install\]/i \
OOMPolicy=continue \
OOMScoreAdjust=-900 \
Delegate=yes \
TasksMax=infinity \
Restart=on-failure \
RestartSec=5s \
Slice=actions-runner.slice' \
    ./bin/actions.runner.service.template
./svc.sh install || true
./svc.sh start || true
