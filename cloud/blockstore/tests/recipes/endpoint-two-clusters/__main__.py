import argparse
import os
import uuid
import logging

from cloud.blockstore.public.sdk.python import client

from library.python.testing.recipe import declare_recipe, set_env


BLOCK_SIZE = 4*1024
BLOCKS_COUNT = 32*1024

logger = logging.getLogger(__name__)


def _get_ipc_type(ipc_type_name):
    if ipc_type_name == "vhost":
        return client.protos.EClientIpcType.IPC_VHOST

    elif ipc_type_name == "nbd":
        return client.protos.EClientIpcType.IPC_NBD

    elif ipc_type_name == "grpc":
        return client.protos.EClientIpcType.IPC_GRPC

    else:
        raise RuntimeError("Invalid ipc type: {}".format(ipc_type_name))


def _get_nbs_port(i):
    port = os.getenv("LOCAL_KIKIMR_INSECURE_NBS_SERVER_PORT_{}".format(i))
    if port is None:
        port = os.getenv("SERVICE_LOCAL_INSECURE_NBS_SERVER_PORT_{}".format(i))
    if port is None:
        port = os.getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT_{}".format(i))

    return port


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket-dir", action="store", default="/tmp")
    parser.add_argument("--ipc-type", action="store", default="vhost")
    parser.add_argument("--vhost-queues", action="store", default=1)
    parser.add_argument("--verbose", action="store_true", default=False)
    args = parser.parse_args(argv)

    cluster_count = int(os.getenv("CLUSTERS_COUNT"))

    for i in range(cluster_count):
        disk_id = uuid.uuid4().hex[:20]
        set_env("NBS_{}_DISK_ID".format(i), disk_id)

        socket = os.path.join(args.socket_dir, disk_id + ".socket")
        socket = os.path.abspath(socket)
        set_env("NBS_{}_VHOST_SOCKET".format(i), socket)

        port = _get_nbs_port(i)

        with client.CreateClient('localhost:' + port) as nbs_client:
            nbs_client.create_volume(
                disk_id=disk_id,
                block_size=BLOCK_SIZE,
                blocks_count=BLOCKS_COUNT)

            nbs_client.start_endpoint(
                unix_socket_path=socket,
                disk_id=disk_id,
                ipc_type=_get_ipc_type(args.ipc_type),
                client_id='test_client_id',
                vhost_queues=args.vhost_queues)


def stop(argv):
    cluster_count = int(os.getenv("CLUSTERS_COUNT"))

    for i in range(cluster_count):
        port = _get_nbs_port(i)
        socket = os.getenv("NBS_{}_VHOST_SOCKET".format(i))
        disk_id = os.getenv("NBS_{}_DISK_ID".format(i))

        if not port:
            return

        with client.CreateClient('localhost:' + port) as nbs_client:
            if socket and os.path.exists(socket):
                nbs_client.stop_endpoint(unix_socket_path=socket)

            if disk_id:
                nbs_client.destroy_volume(disk_id=disk_id)


if __name__ == "__main__":
    declare_recipe(start, stop)
