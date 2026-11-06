#!/usr/bin/env bash
set -e

EXAMPLE_DIR=$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")
source "$EXAMPLE_DIR/prepare_binaries.sh"

show_help() {
    cat << EOF
Usage: ./8-attach-qemu.sh [--cell2] [--] [COMMAND...]
Connect to the QEMU guest or execute a command over SSH.
-h, --help       Display help
--cell2          Connect to cell-2 (default: cell-1)

QEMU_SSH_PORT overrides the default SSH port (8679 or 18679).

Examples:
  ./8-attach-qemu.sh
  ./8-attach-qemu.sh --cell2 'lsblk; fio --version'
EOF
}

cell=${NBS_CELL:-cell-1}
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h | --help)
            show_help
            exit 0
            ;;
        --cell2)
            cell=cell-2
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            break
            ;;
    esac
done

ssh_port=8679
if [[ $cell == cell-2 ]]; then
    ssh_port=18679
fi
ssh_port=${QEMU_SSH_PORT:-$ssh_port}

key_file=$(mktemp)
trap 'rm -f -- "$key_file"' EXIT
# The repository key is group-readable; SSH requires a private copy.
install -m 600 "$REPO_ROOT/cloud/storage/core/tools/testing/qemu/keys/id_rsa" "$key_file"

ssh -F /dev/null \
    -i "$key_file" \
    -p "$ssh_port" \
    -o IdentitiesOnly=yes \
    -o BatchMode=yes \
    -o ConnectTimeout=5 \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    qemu@127.0.0.1 "$@"
