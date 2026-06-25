import json
import os
import shlex

import cloud.filestore.tools.testing.profile_log.common as profile
import yatest.common as common

from cloud.filestore.tests.python.lib.common import flush_logs
from cloud.storage.core.tools.testing.qemu.lib.common import SshToGuest


FILE_SYSTEM_ID = "nfs_test"
FILE_COUNT = 1024
FILES_PER_DIR = 32
PATTERN = "READ_AHEAD_GREP_PATTERN"
PROFILE_REQUESTS = ("CreateHandle", "DescribeData", "ReadData", "ReadBlob")


def _make_payload(index):
    prefix = f"{PATTERN} file={index:04d}\n"
    body = "abcdefghijklmnopqrstuvwxyz0123456789\n" * 24
    return (prefix + body).encode("utf-8")


def _ssh():
    port = int(os.getenv("QEMU_FORWARDING_PORT"))
    ssh_key = os.getenv("QEMU_SSH_KEY")
    return SshToGuest(user="qemu", port=port, key=ssh_key)


def _run_root_script(ssh, script, timeout=120):
    return ssh(f"sudo bash -s <<'BASH'\n{script}\nBASH", timeout=timeout)


def _populate_files(ssh, root):
    script = f"""
set -euo pipefail
root={shlex.quote(root)}
pattern={shlex.quote(PATTERN)}
files={FILE_COUNT}
files_per_dir={FILES_PER_DIR}

rm -rf "$root"
mkdir -p "$root"

for i in $(seq 0 $((files - 1))); do
    dir=$(printf "%s/dir-%03d" "$root" $((i / files_per_dir)))
    mkdir -p "$dir"

    file=$(printf "%s/file-%04d.txt" "$dir" "$i")
    {{
        printf "%s file=%04d\\n" "$pattern" "$i"
        for _ in $(seq 1 24); do
            printf "abcdefghijklmnopqrstuvwxyz0123456789\\n"
        done
    }} > "$file"
done

sync
"""
    _run_root_script(ssh, script)
    return len(_make_payload(0))


def _drop_guest_caches(ssh):
    _run_root_script(ssh, "sync\necho 3 > /proc/sys/vm/drop_caches")


def _run_grep(ssh, root):
    script = f"""
set -euo pipefail
root={shlex.quote(root)}
pattern={shlex.quote(PATTERN)}
files={FILE_COUNT}

start=$(date +%s%N)
matches=$(grep -r "$pattern" "$root" | wc -l)
end=$(date +%s%N)

if [ "$matches" -ne "$files" ]; then
    echo "expected $files grep matches, got $matches" >&2
    exit 1
fi

elapsed_us=$(((end - start) / 1000))
printf 'READ_AHEAD_GREP_RESULT {{"elapsed_us":%s,"matches":%s}}\\n' \\
    "$elapsed_us" "$matches"
"""
    ret = _run_root_script(ssh, script)

    for line in ret.stdout.decode("utf-8").splitlines():
        if line.startswith("READ_AHEAD_GREP_RESULT "):
            return json.loads(line.removeprefix("READ_AHEAD_GREP_RESULT "))

    raise AssertionError(
        "missing grep result line; stdout={}".format(
            ret.stdout.decode("utf-8", errors="replace")))


def _profile_counts(log_name):
    profile_tool_bin_path = common.binary_path(
        "cloud/filestore/tools/analytics/profile_tool/filestore-profile-tool")
    return profile.analyze_profile_log(
        profile_tool_bin_path,
        common.output_path(log_name),
        FILE_SYSTEM_ID)


def _profile_delta(before, after):
    keys = sorted(set(before) | set(after) | set(PROFILE_REQUESTS))
    return {
        key: after.get(key, 0) - before.get(key, 0)
        for key in keys
        if key in PROFILE_REQUESTS or after.get(key, 0) - before.get(key, 0)
    }


def _assert_expected_path(result):
    nfs_delta = result["nfs_profile_delta"]
    vhost_delta = result["vhost_profile_delta"]

    if result["case"] == "enabled":
        assert nfs_delta["DescribeData"] == 0
        assert nfs_delta["ReadData"] == 0
        assert nfs_delta["ReadBlob"] == FILE_COUNT
        assert vhost_delta["ReadData"] == 0
        assert vhost_delta["ReadBlob"] == 0
    else:
        assert nfs_delta["DescribeData"] == 0
        assert nfs_delta["ReadData"] == FILE_COUNT
        assert nfs_delta["ReadBlob"] == FILE_COUNT
        assert vhost_delta["ReadData"] == FILE_COUNT
        assert vhost_delta["ReadBlob"] == 0


def run_grep_read_ahead_benchmark(case_name):
    ssh = _ssh()
    primary_mount = os.getenv("NFS_MOUNT_PATH")
    workload_name = f"read_ahead_grep_{case_name}"
    workload_root = os.path.join(primary_mount, workload_name)

    bytes_per_file = _populate_files(ssh, workload_root)
    _drop_guest_caches(ssh)
    flush_logs()

    initial_vhost_profile = _profile_counts("vhost-profile.log")
    initial_nfs_profile = _profile_counts("nfs-profile.log")

    grep_result = _run_grep(ssh, workload_root)
    flush_logs()

    final_vhost_profile = _profile_counts("vhost-profile.log")
    final_nfs_profile = _profile_counts("nfs-profile.log")

    result = {
        "case": case_name,
        "elapsed_sec": round(grep_result["elapsed_us"] / 1_000_000, 6),
        "files": FILE_COUNT,
        "bytes_per_file": bytes_per_file,
        "matches": grep_result["matches"],
        "nfs_profile_delta": _profile_delta(
            initial_nfs_profile,
            final_nfs_profile),
        "vhost_profile_delta": _profile_delta(
            initial_vhost_profile,
            final_vhost_profile),
    }
    _assert_expected_path(result)

    result_path = common.output_path(f"grep-read-ahead-{case_name}.json")
    with open(result_path, "w") as f:
        json.dump(result, f, sort_keys=True)
        f.write("\n")

    print("READ_AHEAD_GREP_RESULT " + json.dumps(result, sort_keys=True))
