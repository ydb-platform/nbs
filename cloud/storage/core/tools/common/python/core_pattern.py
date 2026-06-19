# -*- coding: utf-8 -*-
import logging
import re

from os.path import abspath, basename, isabs, join


logger = logging.getLogger(__name__)

CORE_FILENAME_MAX_BYTES = 128

CORE_PATTERN_WILDCARD_SPECIFIERS = (
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

TRUNCATION_WILDCARD_SPECIFIER_MAX_BYTES = {
    "%p": 10,
}


def _full_binary_path(binary_path, cwd):
    if isabs(binary_path):
        return binary_path

    if cwd:
        return abspath(join(cwd, binary_path))

    return abspath(binary_path)


def _core_comm(binary_path):
    # Linux truncates %e to the first 15 bytes of task comm.
    return basename(binary_path)[:15] + '*'


def _escape_recover_core_pattern_literal_percent(text):
    return text.replace('%', '%%')


def _wildcard_variable_width_specifiers_for_truncation(pattern):
    wildcard_expansion_extra_bytes = 0
    parts = []
    offset = 0
    for part in re.split(r"(%.)", pattern):
        max_bytes = TRUNCATION_WILDCARD_SPECIFIER_MAX_BYTES.get(part)
        part_bytes = len(part.encode())
        if max_bytes is None or offset >= CORE_FILENAME_MAX_BYTES:
            parts.append(part)
            offset += part_bytes
            continue

        parts.append('*')
        wildcard_expansion_extra_bytes += max_bytes - len('*')
        offset += part_bytes

    return ''.join(parts), wildcard_expansion_extra_bytes


def _truncate_core_pattern(pattern):
    if len(pattern.encode()) <= CORE_FILENAME_MAX_BYTES:
        return pattern

    pattern, wildcard_expansion_extra_bytes = _wildcard_variable_width_specifiers_for_truncation(pattern)

    # core(5): the resulting core filename is limited to 128 bytes on Linux.
    # Keep the returned value a glob because truncation can remove suffix
    # specifiers such as %p after the kernel expands the pattern.
    max_pattern_bytes = max(1, CORE_FILENAME_MAX_BYTES - wildcard_expansion_extra_bytes)
    truncated = pattern.encode()[:max_pattern_bytes - 1]
    while True:
        try:
            return truncated.decode() + '*'
        except UnicodeDecodeError:
            truncated = truncated[:-1]

# Technically we do not need this function, because it is used in Daemon class
# and it itself calls common.execute with core_pattern argument, which, if set 
# to None will automatically use yatool builtin pattern discovery inside 
# `recover_core_dump_file`
# https://github.com/yandex/yatool/blob/main/library/python/cores/__init__.py#L24
# but we have few special cases like cloud/storage/core/tools/testing/unstable-process/
# which launch process, but we want to actually track different process
def core_pattern(binary_path, cwd=None):
    with open("/proc/sys/kernel/core_pattern") as f:
        p = f.read().strip("\n")

        logger.info("System core pattern: %s", p)

        if not p.startswith('|'):
            full_binary_path = _full_binary_path(binary_path, cwd)
            literal_percent = "__CORE_PATTERN_LITERAL_PERCENT__"
            p = p.replace('%%', literal_percent)
            p = p.replace(
                '%E',
                _escape_recover_core_pattern_literal_percent(full_binary_path.replace("/", "!")),
            )
            p = p.replace(
                '%e',
                _escape_recover_core_pattern_literal_percent(_core_comm(binary_path)),
            )
            for specifier in CORE_PATTERN_WILDCARD_SPECIFIERS:
                p = p.replace(specifier, '*')
            p = p.replace(literal_percent, '%%')
            return _truncate_core_pattern(p)

    name = binary_path.replace("/", "_")
    name = name.replace(".", "_")

    return f'/var/lib/apport/coredump/core.{name}.*'
