import logging

from contextlib import contextmanager

from cloud.blockstore.pylibs.ycp import Ycp, YcpWrapper

from .errors import ResourceExhaustedError
from .helpers import wait_for_block_device_to_appear

_logger = logging.getLogger(__file__)


class YcpNewInstancePolicy:
    def __init__(self,
                 ycp: YcpWrapper,
                 zone_id: str,
                 name: str,
                 core_count: int,
                 ram_count: int,
                 compute_node: str,
                 placement_group: str,
                 platform_ids: list[str],
                 do_not_delete_on_error: bool,
                 auto_delete: bool,
                 module_factory,
                 ssh_key_path: str | None = None) -> None:
        self._ycp = ycp
        self._zone_id = zone_id
        self._name = name
        self._core_count = core_count
        self._ram_count = ram_count
        self._compute_node = compute_node
        self._placement_group = placement_group
        self._platform_ids = platform_ids
        self._do_not_delete_on_error = do_not_delete_on_error
        self._auto_delete = auto_delete
        self._module_factory = module_factory
        self._ssh_key_path = ssh_key_path

    @contextmanager
    def obtain(self) -> Ycp.Instance:
        self._instance = None
        for platform_id in self._platform_ids:
            try:
                with self._ycp.create_instance(
                        name=self._name,
                        cores=self._core_count,
                        memory=self._ram_count,
                        compute_node=self._compute_node,
                        placement_group_name=self._placement_group,
                        platform_id=platform_id,
                        auto_delete=False,
                        underlay_vm=self._ycp._folder_desc.create_underlay_vms,
                ) as instance:
                    self._instance = instance
                break
            except Exception as e:
                _logger.info(f'Cannot create VM on platform'
                             f' {platform_id} in zone {self._zone_id}')
                _logger.info(f'Error: {e}')
        if not self._instance:
            raise ResourceExhaustedError(f'Cannot create VM on any platform'
                                         f' from {self._platform_ids} in zone'
                                         f' {self._zone_id}')
        try:
            yield self._instance
        except Exception:
            if self._do_not_delete_on_error:
                _logger.info(f'Instance <id={self._instance.id}> will not be deleted')
                self._auto_delete = False
            raise
        finally:
            if self._auto_delete:
                self._ycp.delete_instance(self._instance)

    @contextmanager
    def attach_disk(self, disk: Ycp.Disk):
        # Temprorary disable autodetach to debug acceptance test failure
        with self._ycp.attach_disk(self._instance, disk, None, False):
            yield wait_for_block_device_to_appear(
                self._instance.ip,
                disk.id,
                self._module_factory,
                self._ssh_key_path,
            )
