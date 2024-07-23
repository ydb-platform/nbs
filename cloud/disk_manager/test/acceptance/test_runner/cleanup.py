import argparse
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Callable, Any

from cloud.blockstore.pylibs.ycp import YcpWrapper
from cloud.disk_manager.test.acceptance.test_runner.lib import (
    size_prettifier,
    make_disk_parameters_string,
)

_logger = logging.getLogger(__file__)


def _get_entity_accessors(ycp):
    return {
        'disk': (ycp.list_disks, ycp.delete_disk),
        'snapshot': (ycp.list_snapshots, ycp.delete_snapshot),
        'image': (ycp.list_images, ycp.delete_image),
    }


def _cleanup_stale_entities(
        entity_accessors: dict[
            str,
            tuple[
                Callable[[], list],
                Callable[[Any], None],
            ],
        ],
        entity_ttls: dict[str, timedelta],
        regexps: dict[str, list[re.Pattern]],
):
    for entity_type, handlers in entity_accessors.items():
        [lister, deleter] = handlers
        for entity in lister():
            ttl = entity_ttls.get(entity_type, None)
            if ttl is None:
                _logger.warning("No ttl for entity %s", entity_type)
                continue
            should_be_older_than = datetime.now(timezone.utc) - ttl
            if entity.created_at > should_be_older_than:
                continue
            for pattern in regexps.get(entity_type, []):
                if re.match(pattern, entity.name):
                    _logger.info(
                        "Deleting %s with name %s",
                        entity_type, entity.name)
                    try:
                        deleter(entity)
                    except Exception as e:
                        _logger.error(
                            "Error while deleting %s with name %s",
                            entity_type,
                            entity.name,
                            exc_info=e,
                        )
                    break


def cleanup_previous_acceptance_tests_results(
        ycp: YcpWrapper,
        test_type: str,
        disk_type: str,
        disk_size: int,
        disk_blocksize: int,
        entity_ttl: timedelta = timedelta(days=1),
):
    entity_accessors = _get_entity_accessors(ycp)
    disk_size = size_prettifier(disk_size).lower()
    disk_blocksize = size_prettifier(disk_blocksize).lower()
    _logger.info(
        "Performing cleanup for %s, disk type %s, disk size %s, disk block size %s",
        test_type,
        disk_type,
        disk_size,
        disk_blocksize,
    )
    regexps = {
        entity_type: [
            re.compile(
                fr'^acc-{entity_type}-'
                fr'{test_type}-'
                fr'{make_disk_parameters_string(disk_type, disk_size, disk_blocksize)}-[0-9]+$',
            ),
            re.compile(fr'^acc-{entity_type}-{test_type}-[0-9]+$'),
            re.compile(
                fr'^acceptance-test-{entity_type}-'
                fr'{test_type}-{disk_size}-{disk_blocksize}-[0-9]+$',
            ),  # Should cleanup for both old and new formats of entity names (#1576)
            re.compile(fr'^acceptance-test-{entity_type}-{test_type}-[0-9]+$')
        ] for entity_type in entity_accessors
    }
    _cleanup_stale_entities(
        entity_accessors,
        {
            'disk': entity_ttl,
            'image': entity_ttl,
            'snapshot': entity_ttl,
        },
        regexps,
    )


class BaseResourceCleaner:
    def __init__(self, ycp: YcpWrapper, args: argparse.Namespace):
        self._ycp = ycp
        self._args = args
        self._disk_type = None
        self._disk_size = None
        self._disk_blocksize = None
        if hasattr(args, 'disk_type'):
            self._disk_type = args.disk_type
        if hasattr(args, 'disk_size'):
            self._disk_size = size_prettifier(
                args.disk_size * (1024 ** 3)).lower()
        if hasattr(args, 'disk_blocksize'):
            self._disk_blocksize = size_prettifier(args.disk_blocksize).lower()
        self._entity_ttls: dict[str, timedelta] = {
            'instance': timedelta(days=1),
            'disk': timedelta(days=1),
            'snapshot': timedelta(days=1),
            'images': timedelta(days=1)
        }
        self._patterns: dict[str, list[re.Pattern]] = {
            'instance':  [
                re.compile(rf'^acc-{args.test_type}-[0-9]+$'),
                re.compile(rf'^acceptance-test-{args.test_type}-[0-9]+$'),
            ],
        }

    def cleanup(self):
        entity_accessors = _get_entity_accessors(self._ycp)
        accessors = {
            'disk': entity_accessors['disk'],
            'snapshot': entity_accessors['snapshot'],
            'instance': (
                self._ycp.list_instances,
                self._ycp.delete_instance,
            )
        }
        _cleanup_stale_entities(
            accessors,
            self._entity_ttls,
            self._patterns,
        )

    @property
    def _disk_parameters_string(self):
        return make_disk_parameters_string(self._disk_type, self._disk_size, self._disk_blocksize)
