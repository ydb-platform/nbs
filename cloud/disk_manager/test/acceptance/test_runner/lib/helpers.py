import contextlib
import fcntl
import logging
import os

import random
import socket
from pathlib import Path

from typing import Callable, Protocol

from .errors import Error

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import get_cluster_test_config
from cloud.blockstore.pylibs.ycp import YcpWrapper, make_ycp_engine

_logger = logging.getLogger(__file__)


def check_ssh_connection(
    ip: str,
    profiler: common.Profiler,
    module_factory,
    ssh_key_path: str | None = None,
) -> None:
    try:
        helpers = module_factory.make_helpers(False)
        helpers.wait_until_instance_becomes_available_via_ssh(ip, ssh_key_path=ssh_key_path)
    except (common.SshException, socket.error) as e:
        profiler.add_ip(ip)
        raise Error(f'failed to connect to remote host {ip} via ssh: {e}')


def wait_for_block_device_to_appear(
    ip: str,
    block_device: str,
    module_factory,
    ssh_key_path: str | None = None,
) -> None:
    helpers = module_factory.make_helpers(False)
    helpers.wait_for_block_device_to_appear(ip, block_device, ssh_key_path=ssh_key_path)


class YcpConfigGeneratorProtocol(Protocol):
    def generate_ycp_config(self, config_path: str | os.PathLike):
        pass


def create_ycp(cluster_name: str,
               zone_id: str,
               chunk_storage_type: str,
               cluster_test_config_path: str | None,
               make_ycp_config_generator: Callable[[bool], YcpConfigGeneratorProtocol],
               module_factory,
               use_generated_config: bool = True,
               profile_name: str | None = None,
               dry_run: bool = False,
               ycp_requests_template_path: str | None = None) -> YcpWrapper:
    cluster = get_cluster_test_config(cluster_name, zone_id, cluster_test_config_path)
    if chunk_storage_type == 'random':
        chunk_storage_type = random.choice(['ydb', 's3'])
        _logger.info(f'"{chunk_storage_type}" is selected as the random chunk storage type')

    return YcpWrapper(profile_name or cluster.name,
                      cluster.chunk_storage_type_to_folder_desc(
                          chunk_storage_type),
                      logger=_logger,
                      engine=make_ycp_engine(dry_run),
                      ycp_config_generator=make_ycp_config_generator(dry_run),
                      helpers=module_factory.make_helpers(dry_run),
                      use_generated_config=use_generated_config,
                      ycp_requests_template_path=ycp_requests_template_path,
                      )


def size_prettifier(size_bytes: int) -> str:
    if size_bytes % (1024 ** 4) == 0:
        return '%sTiB' % (size_bytes // 1024 ** 4)
    elif size_bytes % (1024 ** 3) == 0:
        return '%sGiB' % (size_bytes // 1024 ** 3)
    elif size_bytes % (1024 ** 2) == 0:
        return '%sMiB' % (size_bytes // 1024 ** 2)
    elif size_bytes % 1024 == 0:
        return '%sKiB' % (size_bytes // 1024)
    else:
        return '%sB' % size_bytes


@contextlib.contextmanager
def file_lock(name: str):
    lock_path = Path(f'/tmp/disk_manager_acceptance_lock/{name}.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('w') as f:
        fcntl.lockf(f, fcntl.LOCK_EX)
        yield
