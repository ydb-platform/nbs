from __future__ import annotations

import json
from pathlib import Path

from scripts.tests import ya_build_errors as ybe


def test_collect_configure_errors_deduplicates_and_strips_markup(
    tmp_path: Path,
) -> None:
    evlog = tmp_path / "ya_evlog.jsonl"
    error = {
        "value": {
            "Type": "Error",
            "Where": "$B/contrib/libs/silk/src/fibers/libsilk-src-fibers.a",
            "Sub": "-WBadIncl",
            "Message": (
                "could not resolve include file: [[imp]]fcontext.h[[rst]] "
                "included from here: [[imp]]$S/contrib/libs/silk/src/fibers/fiber.cpp[[rst]]\n"
            ),
        }
    }
    evlog.write_text(
        "\n".join(
            [
                json.dumps(error),
                json.dumps(error),
                json.dumps({"value": {"Type": "Debug", "Message": "ignored"}}),
            ]
        ),
        encoding="utf-8",
    )

    errors = ybe.collect_configure_errors(evlog)
    markdown = ybe.render_markdown(
        configure_errors=errors,
        compiler_blocks=[],
        evlog_url="https://logs/ya_evlog.jsonl",
    )

    assert len(errors) == 1
    assert "[[imp]]" not in markdown
    assert "fcontext.h" in markdown
    assert "$B/contrib/libs/silk/src/fibers/libsilk-src-fibers.a" in markdown
    assert "[ya_evlog.jsonl](https://logs/ya_evlog.jsonl)" in markdown


def test_collect_compiler_error_blocks_limits_blocks_and_lines(tmp_path: Path) -> None:
    log = tmp_path / "ya_log.txt"
    log.write_text(
        "\n".join(
            [
                "2026-05-16 19:23:42,022 DEBUG (yalibrary.runner.runner3) "
                "[MainThread] Task Run(uid$(BUILD_ROOT)/contrib/libs/silk/src/fibers/fiber.cpp.o) "
                "failed with 1 exit code: ",
                "\x1b[1m/actions-runner/_work/nbs/nbs/contrib/libs/silk/src/fibers/fiber.cpp:28:10: "
                "\x1b[0;1;31mfatal error: \x1b[0m\x1b[1m'fcontext.h' file not found\x1b[0m",
                "#include <fcontext.h>",
                "1 error generated.",
                "",
                "2026-05-16 19:23:42,028 DEBUG (yalibrary.runner.runner3) "
                "[MainThread] Task Run(uid$(BUILD_ROOT)/lib.a) failed with 1 exit code: no strderr was provided",
                "2026-05-16 19:23:42,029 DEBUG (yalibrary.runner.runner3) "
                "[MainThread] Task Run(uid$(BUILD_ROOT)/cloud/storage/server/main.cpp.o) failed with 1 exit code: ",
                "cloud/storage/server/main.cpp:184:49: error: data argument not used by format string",
                '            SILK_WARN("recv length failed: {}", ErrnoMsg(r));',
                "cloud/storage/server/main.cpp:184:49: error: cannot pass object of non-trivial type",
                "20 errors generated.",
                "2026-05-16 19:23:42,042 DEBUG (yalibrary.runner.runner3) [MainThread] Merged exit code: 1",
            ]
        ),
        encoding="utf-8",
    )

    blocks = ybe.collect_compiler_error_blocks(
        log,
        max_blocks=2,
        max_lines_per_block=2,
    )
    markdown = ybe.render_markdown(
        configure_errors=[],
        compiler_blocks=blocks,
        ya_make_output_url="https://logs/ya_make_output.txt",
        ya_log_url="https://logs/ya_log.txt",
    )

    assert len(blocks) == 2
    assert "contrib/libs/silk/src/fibers/fiber.cpp:28:10: fatal error:" in markdown
    assert "/actions-runner/_work/nbs/nbs" not in markdown
    assert "no strderr was provided" not in markdown
    assert "... truncated; see ya_log.txt" in markdown
    assert "[ya_make_output.txt](https://logs/ya_make_output.txt)" in markdown
    assert "[ya_log.txt](https://logs/ya_log.txt)" in markdown
