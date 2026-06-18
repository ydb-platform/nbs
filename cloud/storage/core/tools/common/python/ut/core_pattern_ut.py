import builtins
from io import StringIO
from unittest import mock

from cloud.storage.core.tools.common.python.core_pattern import (
    CORE_FILENAME_MAX_BYTES,
    core_pattern,
)


def core_pattern_with_system_pattern(pattern, binary_path, cwd=None):
    real_open = builtins.open

    def open_mock(path, *args, **kwargs):
        if path == "/proc/sys/kernel/core_pattern":
            return StringIO(pattern)
        return real_open(path, *args, **kwargs)

    with mock.patch("builtins.open", open_mock):
        return core_pattern(binary_path, cwd)


def test_short_pattern_is_rendered_without_truncation():
    assert (
        core_pattern_with_system_pattern(
            "/coredumps/%e.%p.%s",
            "/build/bin/storage-service",
        )
        == "/coredumps/storage-service*.%p.*"
    )


def test_literal_percent_is_preserved():
    assert (
        core_pattern_with_system_pattern(
            "/cores/%%.%e.%p",
            "/build/bin/nbs",
        )
        == "/cores/%.nbs*.%p"
    )


def test_long_expanded_pattern_is_truncated_to_kernel_limit():
    binary_path = "/build/" + "/".join(["long-directory-name"] * 12) + "/nbs"

    pattern = core_pattern_with_system_pattern("/cores/%E.%p", binary_path)

    assert len(pattern.encode()) == CORE_FILENAME_MAX_BYTES
    assert pattern.endswith("*")
    assert pattern.startswith("/cores/!build!long-directory-name")
    assert "%p" not in pattern


def test_truncation_does_not_split_utf8_sequence():
    binary_path = "/build/" + "x" * 116 + "/\N{SNOWMAN}"

    pattern = core_pattern_with_system_pattern("/cores/%E.%p", binary_path)

    assert len(pattern.encode()) <= CORE_FILENAME_MAX_BYTES
    assert pattern.endswith("*")
