import argparse
import logging
import os

import yatest.common as common

from library.python.testing.recipe import declare_recipe, set_env

from cloud.filestore.tests.python.lib.client import FilestoreCliClient, create_endpoint


logger = logging.getLogger(__name__)


def env_with_index(env, index):
    if index == 0:
        return env

    return "{}__{}".format(env, index)


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--filesystem", action="store", default="nfs_share")
    parser.add_argument("--socket-path", action="store", default="/tmp")
    parser.add_argument("--socket-prefix", action="store", default="test.vhost")
    parser.add_argument("--mount-seqno", action="store", default=0)
    parser.add_argument("--shard-count", action="store", default=0, type=int)
    parser.add_argument("--read-only", action="store_true", default=False)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--endpoint-count", action="store", default=1, type=int)
    parser.add_argument("--vhost-queue-count", action="store", default=0, type=int)
    args = parser.parse_args(argv)

    port = os.getenv("NFS_SERVER_PORT")
    vhost_port = os.getenv("NFS_VHOST_PORT")

    # Create filestore
    client_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")

    client = FilestoreCliClient(
        client_path,
        port,
        vhost_port=vhost_port,
        verbose=args.verbose,
        cwd=common.output_path())

    client.create(args.filesystem, "test_cloud", "test_folder")

    if args.shard_count > 0:
        shards = []
        shard_ids = [
            args.filesystem + "_shard_" + str(i) for i in range(args.shard_count)
        ]
        for i, shard_id in enumerate(shard_ids):
            shard_id = args.filesystem + "_shard_" + str(i)
            client.create(shard_id, "test_cloud", "test_folder")
            shards.append(shard_id)
            client.execute_action("configureasshard", {
                "FileSystemId": shard_id,
                "ShardNo": i + 1,
                "ShardFileSystemIds": shard_ids,
                "MainFileSystemId": args.filesystem,
            })

        client.execute_action("configureshards", {
            "FileSystemId": args.filesystem,
            "ShardFileSystemIds": shards,
        })

    set_env("NFS_VHOST_SOCKET_COUNT", args.endpoint_count)
    for i in range(args.endpoint_count):
        socket = create_endpoint(
            client,
            args.filesystem,
            args.socket_path,
            args.socket_prefix,
            os.getenv("NFS_VHOST_ENDPOINT_STORAGE_DIR", None),
            args.mount_seqno,
            args.read_only,
            client_id=f"localhost@{i}",
            vhost_queue_count=args.vhost_queue_count)

        set_env(env_with_index("NFS_VHOST_SOCKET", i), socket)


def stop(argv):
    vhost_port = os.getenv("NFS_VHOST_PORT")
    endpoint_count = int(os.getenv("NFS_VHOST_SOCKET_COUNT"))
    for i in range(endpoint_count):
        socket = os.getenv(env_with_index("NFS_VHOST_SOCKET", i))

        if not vhost_port or not socket or not os.path.exists(socket):
            return

        client_path = common.binary_path(
            "cloud/filestore/apps/client/filestore-client")

        client = FilestoreCliClient(
            client_path, port=None,
            vhost_port=vhost_port,
            verbose=True,
            cwd=common.output_path())
        client.stop_endpoint(socket)


if __name__ == "__main__":
    declare_recipe(start, stop)
