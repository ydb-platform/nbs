# -*- coding: utf-8 -*-
import logging

from os.path import basename


logger = logging.getLogger(__name__)


def core_pattern(binary_path, cwd=None):
    with open("/proc/sys/kernel/core_pattern") as f:
        p = f.read().strip("\n")

        logger.info("System core pattern: %s", p)

        if not p.startswith('|'):
            mask = basename(binary_path)[:8] + '*'
            return p.replace('%e', mask)

    name = binary_path.replace("/", "_")
    name = name.replace(".", "_")

    return f'/var/lib/apport/coredump/core.{name}.*'
