import json
import logging
import os

import random
import shlex
import socket
import time

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
    disk_id: str,
    module_factory,
    ssh_key_path: str | None = None,
):
    device_name = f'/dev/disk/by-id/virtio-{disk_id}'
    helpers = module_factory.make_helpers(False)
    helpers.wait_for_block_device_to_appear(ip, device_name, ssh_key_path=ssh_key_path)


class VirtualDevicesToIdMapper:
    def __init__(
        self,
        ip: str,
        module_factory,
        ssh_key_path: str | None = None,
    ):
        self._ip = ip
        self._module_factory = module_factory
        self._ssh_key_path = ssh_key_path

    def _execute_command(self, args: list[str]) -> str:
        with self._module_factory.make_ssh_client(False, self._ip, ssh_key_path=self._ssh_key_path) as ssh:
            _, stdout, stderr = ssh.exec_command(shlex.join(args))
            output = ''
            for line in iter(lambda: stdout.readline(2048), ''):
                output += line
                _logger.info("stdout: %s", line.rstrip())
            if stderr.channel.recv_exit_status():
                stderr_lines = stderr.readlines()
                stderr_str = ''.join(stderr_lines)
                for stderr_line in stderr_lines:
                    _logger.info("stderr: %s ", stderr_line.rstrip())
                raise Error(f'failed to execute command {args} on remote host'
                            f' {self._ip}: {stderr_str}')
            return output

    def get_virtual_disks_mapping(self) -> dict[str, str]:
        lsblk_output = self._execute_command(["lsblk",  "--json", "-lpn", "-d", "-o", "name,type,subsystems"])
        virtual_disk_paths = self.list_virtual_disks(lsblk_output)
        _logger.info("Collected virtual devices: %s", ', '.join(virtual_disk_paths))
        udevadm_output = self._execute_command(
            [
                "udevadm",
                "info",
                *[f"--name={vdisk_path}" for vdisk_path in virtual_disk_paths],
            ]
        )
        return self.get_disk_id_to_device_path_mapping(udevadm_output)

    def wait_for_disk_to_appear(self, disk_id: str, timeout_sec: int = 120) -> str:
        _logger.info(
            "Started waiting for disk %s to appear in the guest system, waiting for %d",
            disk_id,
            timeout_sec,
        )
        started_at = time.monotonic()

        while True:

            try:
                mapping = self.get_virtual_disks_mapping()
            except Exception as e:
                _logger.error("Error while enumerating virtual devices", exc_info=e)
                time.sleep(1)
                continue

            if disk_id in mapping:
                result = mapping[disk_id]
                _logger.info(
                    "Successfully found disk: %s, virtual device path %s",
                    disk_id,
                    result,
                )
                return result

            if time.monotonic() - started_at > timeout_sec:
                break

            time.sleep(1)

        raise TimeoutError(
            f"Error while waiting for the disk {disk_id} to appear timeout {timeout_sec} seconds expired",
        )

    @staticmethod
    def list_virtual_disks(lsblk_output: str) -> list[str]:
        result = []
        record = json.loads(lsblk_output)

        for disk in record['blockdevices']:

            if disk.get('type') != 'disk':
                continue

            if disk.get('subsystems', '') != 'block:virtio:pci':
                continue

            result.append(disk['name'])
        return result

    @staticmethod
    def get_disk_id_to_device_path_mapping(udevadm_output: str) -> dict[str, str]:
        result = {}
        disk_record_line_groups = udevadm_output.split("\n\n")

        for disk_record_line_group in disk_record_line_groups:
            current_record = {}

            for line in disk_record_line_group.splitlines():
                if "=" not in line:
                    continue
                [_, field] = line.split(" ", maxsplit=1)
                [key, value] = field.split("=")
                current_record[key] = value

            if "DEVNAME" not in current_record:
                continue

            if "ID_SERIAL" not in current_record:
                continue

            result[current_record["ID_SERIAL"]] = current_record["DEVNAME"]

        return result


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


def shorten_disk_type(disk_type: str) -> str:
    match disk_type:
        case "network-ssd-nonreplicated":
            return "ssd-nonrepl"
        case "network-hdd-nonreplicated":
            return "hdd-nonrepl"
        case "network-ssd-io-m2":
            return "ssd-io-m2"
        case "network-ssd-io-m3":
            return "ssd-io-m3"
        case _:
            return disk_type


# Example of disk parameters string: network-ssd-1tib-4kib
def make_disk_parameters_string(
        disk_type: str,
        size: int | str,
        block_size: int | str,
        delim: str = "-") -> str:
    disk_type = shorten_disk_type(disk_type)
    if not isinstance(size, str):
        size = size_prettifier(size)
    if not isinstance(block_size, str):
        block_size = size_prettifier(block_size)
    return f'{disk_type}{delim}{size}{delim}{block_size}'.lower()
