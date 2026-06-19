import builtins
import fnmatch
import re
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


def recover_core_mask_like_yatest(core_mask, pid):
    def resolve(text):
        if text == "%p":
            return str(pid)
        if text == "%%":
            return "%"
        if text.startswith("%"):
            return "*"
        return text

    return "".join(resolve(p) for p in filter(None, re.split(r"(%.)", core_mask)))


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
        == "/cores/%%.nbs*.%p"
    )


def test_literal_percent_in_executable_path_is_preserved():
    assert (
        core_pattern_with_system_pattern(
            "/cores/%E.%p",
            "/build/foo%p/nbs",
        )
        == "/cores/!build!foo%%p!nbs.%p"
    )


def test_percent_p_in_executable_path_is_not_treated_as_pid_specifier():
    binary_path = "/build/foo%p/nbs"
    pid = 12345
    core_filename = "/cores/!build!foo%p!nbs.12345"

    pattern = core_pattern_with_system_pattern("/cores/%E.%p", binary_path)
    resolved_pattern = recover_core_mask_like_yatest(pattern, pid)

    naive_pattern = "/cores/!build!foo%p!nbs.%p"
    resolved_naive_pattern = recover_core_mask_like_yatest(naive_pattern, pid)

    assert resolved_naive_pattern == "/cores/!build!foo12345!nbs.12345"
    assert not fnmatch.fnmatch(core_filename, resolved_naive_pattern)
    assert resolved_pattern == core_filename


def test_long_expanded_pattern_is_truncated_to_kernel_limit():
    binary_path = "/build/" + "/".join(["long-directory-name"] * 12) + "/nbs"

    pattern = core_pattern_with_system_pattern("/cores/%E.%p", binary_path)

    assert len(pattern.encode()) == CORE_FILENAME_MAX_BYTES
    assert pattern.endswith("*")
    assert pattern.startswith("/cores/!build!long-directory-name")
    assert "%p" not in pattern


def test_variable_width_pid_before_long_expanded_pattern_matches_kernel_truncation():
    binary_path = "/build/" + "/".join(["long-directory-name"] * 12) + "/nbs"
    pid = "1234567"

    pattern = core_pattern_with_system_pattern("/cores/%p.%E", binary_path)
    core_filename = f"/cores/{pid}.{binary_path.replace('/', '!')}"
    core_filename = core_filename.encode()[:CORE_FILENAME_MAX_BYTES].decode()

    assert len(pattern.encode()) <= CORE_FILENAME_MAX_BYTES
    assert pattern.startswith("/cores/*.")
    assert "%p" not in pattern
    assert fnmatch.fnmatch(core_filename, pattern)


def test_naive_truncation_before_pid_expansion_would_miss_core():
    binary_path = "/build/" + "/".join(["long-directory-name"] * 12) + "/nbs"
    pid = "1234567890"
    encoded_binary_path = binary_path.replace('/', '!')

    core_filename = f"/cores/{pid}.{encoded_binary_path}"
    core_filename = core_filename.encode()[:CORE_FILENAME_MAX_BYTES].decode()

    naive_pattern = f"/cores/%p.{encoded_binary_path}"
    naive_pattern = naive_pattern.encode()[:CORE_FILENAME_MAX_BYTES - 1].decode() + '*'
    naive_pattern = naive_pattern.replace("%p", pid)

    pattern = core_pattern_with_system_pattern("/cores/%p.%E", binary_path)

    assert not fnmatch.fnmatch(core_filename, naive_pattern)
    assert fnmatch.fnmatch(core_filename, pattern)


def test_truncation_does_not_split_utf8_sequence():
    binary_path = "/build/" + "x" * 116 + "/\N{SNOWMAN}"

    pattern = core_pattern_with_system_pattern("/cores/%E.%p", binary_path)

    assert len(pattern.encode()) <= CORE_FILENAME_MAX_BYTES
    assert pattern.endswith("*")
