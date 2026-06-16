# -*- coding: utf-8 -*-
import logging

from os.path import abspath, basename, isabs, join


logger = logging.getLogger(__name__)

CORE_PATTERN_WILDCARD_SPECIFIERS = (
    "%p",
    "%P",
    "%i",
    "%I",
    "%u",
    "%g",
    "%d",
    "%s",
    "%t",
    "%h",
    "%b",
    "%c",
    "%F",
)


def _full_binary_path(binary_path, cwd):
    if isabs(binary_path):
        return binary_path

    if cwd:
        return abspath(join(cwd, binary_path))

    return abspath(binary_path)


def core_pattern(binary_path, cwd=None):
    with open("/proc/sys/kernel/core_pattern") as f:
        p = f.read().strip("\n")

        logger.info("System core pattern: %s", p)

        if not p.startswith('|'):
            full_binary_path = _full_binary_path(binary_path, cwd)
            mask = basename(binary_path)[:8] + '*'
            literal_percent = "__CORE_PATTERN_LITERAL_PERCENT__"
            p = p.replace('%%', literal_percent)
            p = p.replace('%E', full_binary_path.replace("/", "!"))
            p = p.replace('%e', mask)
            for specifier in CORE_PATTERN_WILDCARD_SPECIFIERS:
                p = p.replace(specifier, '*')
            return p.replace(literal_percent, '%')

    name = binary_path.replace("/", "_")
    name = name.replace(".", "_")

    return f'/var/lib/apport/coredump/core.{name}.*'
