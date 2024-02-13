import logging
import re

from abc import ABC, abstractmethod
from contextlib import contextmanager

from cloud.blockstore.pylibs.ycp import Ycp, YcpWrapper

from .errors import (
    Error,
    ResourceExhaustedError,
)

_logger = logging.getLogger(__file__)


class DiskPolicy(ABC):

    @abstractmethod
    def obtain(self) -> Ycp.Disk:
        """Obtain (get or create) disk"""


class YcpNewDiskPolicy(DiskPolicy):
    def __init__(self,
                 ycp: YcpWrapper,
                 zone_id: str,
                 name: str,
                 size: str,
                 blocksize: str,
                 type_id: str,
                 auto_delete: str) -> None:
        self._ycp = ycp
        self._zone_id = zone_id
        self._name = name
        self._size = size
        self._blocksize = blocksize
        self._type_id = type_id
        self._auto_delete = auto_delete

    @contextmanager
    def obtain(self) -> Ycp.Disk:
        try:
            with self._ycp.create_disk(
                name=self._name,
                type_id=self._type_id,
                bs=self._blocksize,
                size=self._size,
                auto_delete=self._auto_delete
            ) as disk:
                yield disk
        except Exception as e:
            _logger.info(f'Error: {e}')
            raise ResourceExhaustedError(f'Cannot create disk in zone'
                                         f' {self._zone_id}')


class YcpFindDiskPolicy(DiskPolicy):
    def __init__(self, ycp: YcpWrapper, zone_id: str, name_regex: str) -> None:
        self._ycp = ycp
        self._zone_id = zone_id
        self._name_regex = name_regex

    def obtain(self) -> Ycp.Disk:
        disks = sorted(
            self._ycp.list_disks(),
            key=lambda: disk.created_at,
            reverse=True,
        )
        for disk in disks:
            if re.match(self._name_regex, disk.name) and disk.zone_id == self._zone_id:
                return disk
        raise Error(f'Failed to find disk with name regex {self._name_regex}')
