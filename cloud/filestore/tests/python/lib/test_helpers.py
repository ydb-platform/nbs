import os
from google.protobuf import text_format
from cloud.filestore.config.storage_pb2 import TStorageConfig


def get_restart_interval():
    restart_interval = os.getenv("NFS_RESTART_INTERVAL")
    assert (
        restart_interval is not None and restart_interval.isdigit()
    ), "NFS_RESTART_INTERVAL is not set to a valid value"
    return int(restart_interval)


def get_nfs_config_dir(name: str) -> str:
    configs_dir = os.getenv("NFS_CONFIG_DIR")
    assert configs_dir is not None, "NFS_CONFIG_DIR is not set"
    return os.path.join(configs_dir, name)


def get_storage_config_path() -> str:
    return get_nfs_config_dir("storage.txt")


def get_storage_config() -> TStorageConfig:
    storage_config = TStorageConfig()
    with open(get_storage_config_path()) as p:
        storage_config = text_format.Parse(p.read(), TStorageConfig())
    return storage_config


def write_storage_config(storage_config: TStorageConfig):
    with open(get_storage_config_path(), "w") as f:
        f.write(text_format.MessageToString(storage_config))
