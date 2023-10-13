import os

from cloud.blockstore.public.sdk.python import client

from library.python.testing.recipe import declare_recipe


OLD_BLOCKS_COUNT = 32*1024
NEW_BLOCKS_COUNT = 64*1024


def _get_nbs_port():
    port = os.getenv("LOCAL_KIKIMR_INSECURE_NBS_SERVER_PORT")
    if port is None:
        port = os.getenv("SERVICE_LOCAL_INSECURE_NBS_SERVER_PORT")
    if port is None:
        port = os.getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

    return port


def start(argv):
    socket = os.getenv("NBS_VHOST_SOCKET")
    disk_id = os.getenv("NBS_DISK_ID")

    port = _get_nbs_port()

    with client.CreateClient('localhost:' + port) as nbs_client:
        volume = nbs_client.describe_volume(disk_id)
        assert volume.BlocksCount == OLD_BLOCKS_COUNT

        nbs_client.resize_volume(
            disk_id=disk_id,
            blocks_count=NEW_BLOCKS_COUNT,
            channels_count=0,
            config_version=None)

        volume = nbs_client.describe_volume(disk_id)
        assert volume.BlocksCount == NEW_BLOCKS_COUNT

        nbs_client.refresh_endpoint(unix_socket_path=socket)


def stop(argv):
    pass


if __name__ == "__main__":
    declare_recipe(start, stop)
