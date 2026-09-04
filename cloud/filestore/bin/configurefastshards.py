#!/usr/bin/env python3
#
# Reconfigures the last FILE_SHARD_COUNT of the SHARD_COUNT shards created by
# 'initctl.sh create' as file shards backed by persistent fastshard, same
# scheme as cloud/filestore/tests/python/lib/fastshard.py.
#
# Device provisioning: creates one mirrored NBS volume (the example/ NBS setup
# must be running: nbsd + disk agents with
# JournalledDeviceTcpServerListenAddress configured), reads its device layout
# from the DiskRegistry and pairs the k-th device of every replica into the
# k-th fastshard storage group - so each file shard mirrors across the same
# devices NBS allocated for the volume. The volume itself is only an
# allocation holder and must not be mounted.
#
# Requires FastShardRuntimeEnabled: true in nfs/nfs-storage.txt.
#
# All options can be set via the same environment variables the other bin/
# scripts use (FS, SHARD_COUNT, SERVER_PORT, ...) or via command line flags.

import argparse
import json
import os
import re
import subprocess
import sys

BIN_DIR = os.path.dirname(os.path.realpath(__file__))

DEFAULT_BLOCKSTORE_CLIENT = \
    os.path.normpath(os.path.join(BIN_DIR, "blockstore-client"))


def parse_args():
    def env(name, default):
        return os.environ.get(name, default)

    p = argparse.ArgumentParser(
        description="configure persistent fastshard file shards on top of "
                    "devices of a mirrored NBS volume")
    p.add_argument(
        "--fs", default=env("FS", "nfs"),
        help="main filesystem id")
    p.add_argument(
        "--shard-count", type=int, default=int(env("SHARD_COUNT", "8")),
        help="total shard count of the filesystem")
    p.add_argument(
        "--file-shard-count", type=int,
        default=int(env("FILE_SHARD_COUNT", "0")),
        help="number of trailing shards to turn into fastshard file shards"
             " (default: shard-count - 1, the maximum the tablet allows)")
    p.add_argument(
        "--server-port", default=env("SERVER_PORT", "9021"),
        help="filestore-server port")
    p.add_argument(
        "--nbs-server-port", default=env("NBS_SERVER_PORT", "9766"),
        help="nbsd server port")
    p.add_argument(
        "--blockstore-client",
        default=env("BLOCKSTORE_CLIENT", DEFAULT_BLOCKSTORE_CLIENT),
        help="path to the blockstore-client binary")
    p.add_argument(
        "--disk-id", default=env("FASTSHARD_DISK_ID", "fastshard0"),
        help="disk id of the allocation-holder volume")
    p.add_argument(
        "--media-kind", default=env("FASTSHARD_MEDIA_KIND", "mirror2"),
        help="storage media kind of the volume (mirror2|mirror3)")
    p.add_argument(
        "--blocks-per-shard", type=int,
        default=int(env("FASTSHARD_BLOCKS_PER_SHARD", "262144")),
        help="volume blocks (4KiB) per file shard; the default is 1GiB -"
             " one allocation unit of the example/ setup")
    p.add_argument(
        "--nodes-per-group", type=int,
        default=int(env("FASTSHARD_NODES_PER_GROUP", "10000")),
        help="NodesPerGroup for the fastshard config")
    p.add_argument(
        "--group-capacity", type=int,
        default=int(env("FASTSHARD_GROUP_CAPACITY", str(512 * 1024**2))),
        help="ExpectedGroupCapacity in bytes for the fastshard config")
    p.add_argument(
        "--storage-group-type",
        choices=["E_SG_MIRROR", "E_SG_QUORUM_MIRROR"],
        default=env("STORAGE_GROUP_TYPE", "E_SG_MIRROR"),
        help="storage group implementation for the fastshards "
             "(default: %(default)s)")
    p.add_argument(
        "--jd-port-base", type=int, default=int(env("JD_PORT_BASE", "29900")),
        help="journalled device server port convention: agent 'remoteN.*'"
             " listens on base+N, the local agent on base"
             " (see example/0-setup.sh)")

    args = p.parse_args()
    if args.file_shard_count == 0:
        args.file_shard_count = args.shard_count - 1
    return args


def run(cmd, **kwargs):
    print("+ " + " ".join(cmd))
    return subprocess.run(cmd, **kwargs)


def warn_if_fastshard_runtime_disabled():
    storage_config = os.path.join(BIN_DIR, "nfs", "nfs-storage.txt")
    try:
        with open(storage_config) as f:
            if "FastShardRuntimeEnabled" in f.read():
                return
    except OSError:
        pass

    print("WARNING: FastShardRuntimeEnabled is not set in %s;"
          % storage_config)
    print("         persistent fastshards will fall back to stubs. Add")
    print("         'FastShardRuntimeEnabled: true' and restart"
          " filestore-server.")


def create_volume(args):
    print("creating mirrored volume %s" % args.disk_id)
    res = run([
        args.blockstore_client, "createvolume",
        "--port", args.nbs_server_port,
        "--disk-id", args.disk_id,
        "--storage-media-kind", args.media_kind,
        "--blocks-count", str(args.file_shard_count * args.blocks_per_shard),
        "--block-size", "4096",
        "--cloud-id", "cloud",
        "--folder-id", "folder",
    ])
    if res.returncode != 0:
        print("volume creation failed (already exists?)"
              " - using the existing one")


def describe_disk(args):
    res = run([
        args.blockstore_client, "executeaction",
        "--port", args.nbs_server_port,
        "--verbose", "error",
        "--action", "diskregistrydescribedisk",
        "--input-bytes", json.dumps({"DiskId": args.disk_id}),
    ], stdout=subprocess.PIPE, check=True, text=True)
    return json.loads(res.stdout)


def agent_port(agent_id, port_base):
    m = re.match(r"remote(\d+)\.", agent_id)
    return port_base + int(m.group(1)) if m else port_base


def build_storage_groups(args, describe_response):
    def get(d, *names):
        for n in names:
            if n in d:
                return d[n]
        return []

    replicas = [get(describe_response, "Devices", "devices")]
    for r in get(describe_response, "Replicas", "replicas"):
        replicas.append(get(r, "Devices", "devices"))

    groups = []
    for k in range(args.file_shard_count):
        devices = []
        for replica in replicas:
            if k >= len(replica):
                sys.exit("replica has no device for shard %d" % k)
            d = replica[k]
            devices.append({
                "Host": "localhost",
                "Port": agent_port(
                    d.get("AgentId") or d.get("agentId") or "",
                    args.jd_port_base),
                "DeviceId": d.get("DeviceUUID") or d.get("deviceUUID"),
            })
        groups.append([{
            "Devices": devices,
            "Type": args.storage_group_type,
        }])
    return groups


def filestore_execute_action(args, action, request):
    run([
        os.path.join(BIN_DIR, "filestore-client"), "executeaction",
        "--server-port", args.server_port,
        "--action", action,
        "--input-json", json.dumps(request),
    ], check=True)


def configure_shards(args, groups):
    shard_ids = [
        "%s_s%d" % (args.fs, i) for i in range(1, args.shard_count + 1)]
    file_shard_ids = shard_ids[args.shard_count - args.file_shard_count:]

    for i, shard_id in enumerate(shard_ids):
        shard_no = i + 1
        is_fast = shard_id in file_shard_ids
        if is_fast:
            # The pair index is the shard position counted from the tail,
            # so reconfiguring with a different file-shard-count never
            # reassigns devices of the shards that stay file shards.
            fast_config = {"PersistentConfig": {
                "StorageGroups": groups[args.shard_count - shard_no],
                "NodesPerGroup": args.nodes_per_group,
                "ExpectedGroupCapacity": args.group_capacity,
            }}
        else:
            fast_config = {"MemConfig": {}}

        filestore_execute_action(args, "configureasshard", {
            "FileSystemId": shard_id,
            "ShardNo": shard_no,
            "MainFileSystemId": args.fs,
            "ShardFileSystemIds": shard_ids,
            "FileShardFileSystemIds": file_shard_ids,
            "IsFastShard": is_fast,
            "FastShardConfig": fast_config,
            "DirectoryCreationInShardsEnabled": True,
        })
        print("configured shard %s (fastshard: %s)" % (shard_id, is_fast))

    filestore_execute_action(args, "configureshards", {
        "FileSystemId": args.fs,
        "ShardFileSystemIds": shard_ids,
        "FileShardFileSystemIds": file_shard_ids,
        "DirectoryCreationInShardsEnabled": True,
    })
    print("configured %s: %d shards, last %d are fastshard file shards"
          % (args.fs, args.shard_count, args.file_shard_count))


def main():
    args = parse_args()

    if args.file_shard_count >= args.shard_count:
        sys.exit(
            "ERROR: FILE_SHARD_COUNT (%d) must be less than SHARD_COUNT (%d):"
            " the filesystem needs at least one non-file shard"
            % (args.file_shard_count, args.shard_count))

    warn_if_fastshard_runtime_disabled()
    create_volume(args)
    groups = build_storage_groups(args, describe_disk(args))
    configure_shards(args, groups)


if __name__ == "__main__":
    main()
