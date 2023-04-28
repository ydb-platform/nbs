# -*- coding: utf-8 -*-
import logging

from os.path import basename, join


logger = logging.getLogger(__name__)


def core_pattern(binary_path, cwd=None):
    mask = basename(binary_path)[:8] + '*'

    with open("/proc/sys/kernel/core_pattern") as f:
        p = f.read().strip("\n")

        logger.info("System core pattern: %s", p)

        if not p.startswith('|'):
            return p.replace('%e', mask)

    # see: https://a.yandex-team.ru/arc/trunk/arcadia/sandbox/bin/coredumper.py?rev=r5646070#L28
    # > filename = ".".join((name, pid, sig))

    name = ".".join((mask, '%p', '%s'))

    # path = cwd or os.getcwd()
    path = '/coredumps'

    return join(path, name)
