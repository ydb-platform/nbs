#!/usr/bin/env python3
"""Fail-closed service launcher for a reader-compatible NBS host cohort.

Install this launcher and policy under root-owned, non-writable parents. All
eligible hosts (including local-mount and fallback hosts) must use it as their
only service entry point. This is a deployment fence, not tablet metadata.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def open_trusted(path):
    path = Path(path)
    require(path.is_absolute(), "absolute paths are required")
    # Reject symlinks and writable directory components. A trusted deploy owner
    # may replace the policy atomically; an NBS service account cannot.
    for part in [path, *path.parents]:
        info = part.lstat()
        require(not stat.S_ISLNK(info.st_mode), "symlink in trusted path")
        require(info.st_uid == 0 and not info.st_mode & 0o022,
                "path must be root-owned and not writable by group/others")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    info = os.fstat(fd)
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and
            not info.st_mode & 0o022, "untrusted file")
    return fd


def digest_fd(fd):
    h = hashlib.sha256()
    while True:
        chunk = os.read(fd, 1024 * 1024)
        if not chunk:
            break
        h.update(chunk)
    os.lseek(fd, 0, os.SEEK_SET)
    return h.hexdigest()


def verify_policy(policy, binary_sha, hostname):
    require(policy.get("format") == 1, "unsupported policy format")
    require(policy.get("minimum_blob_reader_version", 0) >= 1,
            "reader floor missing")
    require(hostname in policy.get("allowed_hosts", []), "host outside compatible cohort")
    require(binary_sha in policy.get("reader_binary_sha256", []),
            "binary is not approved for compressed Merged v1")
    require(all(len(x) == 64 and all(c in "0123456789abcdef" for c in x)
                for x in policy["reader_binary_sha256"]), "invalid digest list")
    require(policy.get("cohort_id"), "cohort identity missing")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    policy_fd = open_trusted(args.policy)
    with os.fdopen(policy_fd) as f:
        policy = json.load(f)
    fd = open_trusted(args.binary)
    require(os.read(fd, 4) == b"\x7fELF", "binary must be ELF")
    os.lseek(fd, 0, os.SEEK_SET)
    binary_sha = digest_fd(fd)
    verify_policy(policy, binary_sha, os.uname().nodename)
    print(json.dumps({"reader_fence": "verified", "binary_sha256": binary_sha,
                      "cohort_id": policy["cohort_id"]}), file=sys.stderr, flush=True)
    if args.check_only:
        os.close(fd)
        return
    require(os.execve in os.supports_fd, "fd exec unsupported on this host")
    server_args = args.args[1:] if args.args[:1] == ["--"] else args.args
    # Execute the same open inode that was verified, not a mutable path.
    # The policy must remain enforced after writer percentages return to zero.
    require(not any(key.startswith("LD_") for key in os.environ),
            "dynamic-loader overrides are forbidden")
    os.execve(fd, [args.binary, *server_args], os.environ.copy())


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError) as e:
        print("NBS reader fence denied start: " + str(e), file=sys.stderr)
        sys.exit(78)
