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
    html = ybe.render_html(
        configure_errors=errors,
        compiler_blocks=[],
        evlog_url="https://logs/ya_evlog.jsonl",
    )

    assert len(errors) == 1
    assert "[[imp]]" not in html
    assert "fcontext.h" in html
    assert "$B/contrib/libs/silk/src/fibers/libsilk-src-fibers.a" in html
    assert '<a href="https://logs/ya_evlog.jsonl">ya_evlog.jsonl</a>' in html


def test_collect_compiler_error_blocks_limits_blocks_and_lines(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITHUB_WORKSPACE", "/actions-runner/_work/nbs/nbs")

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
    html = ybe.render_html(
        configure_errors=[],
        compiler_blocks=blocks,
        ya_make_output_url="https://logs/ya_make_output.txt",
        ya_log_url="https://logs/ya_log.txt",
    )

    assert len(blocks) == 2
    assert "contrib/libs/silk/src/fibers/fiber.cpp:28:10: fatal error:" in html
    assert "/actions-runner/_work/nbs/nbs" not in html
    assert "no strderr was provided" not in html
    assert "... truncated; see ya_log.txt" in html
    assert '<a href="https://logs/ya_make_output.txt">ya_make_output.txt</a>' in html
    assert '<a href="https://logs/ya_log.txt">ya_log.txt</a>' in html


def test_render_html_matches_failed_build_targets_to_compiler_blocks(
    tmp_path: Path,
) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text(
        """<testsuites>
    <testsuite name="cloud/blockstore/libs/common/ut" tests="0" failures="1">
        <testcase name="unittest">
            <failure>skipped due to a failed build
Depends on broken: cloud/blockstore/libs/common/ut</failure>
        </testcase>
    </testsuite>
</testsuites>""",
        encoding="utf-8",
    )
    log = tmp_path / "ya_log.txt"
    log.write_text(
        "\n".join(
            [
                "2026-05-16 19:23:42,022 DEBUG (yalibrary.runner.runner3) "
                "[MainThread] Task Run(uid$(BUILD_ROOT)/cloud/storage/core/libs/common/ut/__/error_ut.cpp.o) "
                "failed with 1 exit code: ",
                "cloud/storage/core/libs/common/error_ut.cpp:6:2: error: storage failed",
                "2026-05-16 19:23:42,029 DEBUG (yalibrary.runner.runner3) "
                "[MainThread] Task Run(uid$(BUILD_ROOT)/cloud/blockstore/libs/common/ut/__/block_range_ut.cpp.o) "
                "failed with 1 exit code: ",
                "cloud/blockstore/libs/common/block_range_ut.cpp:5:2: error: blockstore failed",
                "1 error generated.",
                "2026-05-16 19:23:42,042 DEBUG (yalibrary.runner.runner3) [MainThread] Merged exit code: 1",
            ]
        ),
        encoding="utf-8",
    )

    targets = ybe.collect_failed_build_targets(junit)
    blocks = ybe.collect_compiler_error_blocks(log, max_blocks=10)
    html = ybe.render_html(
        configure_errors=[],
        compiler_blocks=blocks,
        failed_build_targets=targets,
        only_if_junit_failed_build=True,
    )

    assert [target.name for target in targets] == ["cloud/blockstore/libs/common/ut"]
    assert 'id="cloud-blockstore-libs-common-ut"' in html
    assert "cloud/blockstore/libs/common/ut" in html
    assert "blockstore failed" in html
    assert "storage failed" in html
    assert html.index("cloud/blockstore/libs/common/ut") < html.index(
        "blockstore failed"
    )


def test_render_html_empty_when_junit_has_no_failed_build(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text(
        '<testsuite name="suite"><testcase name="ok"/></testsuite>',
        encoding="utf-8",
    )

    assert ybe.collect_failed_build_targets(junit) == []
    assert (
        ybe.render_html(
            configure_errors=[],
            compiler_blocks=[
                ybe.CompilerErrorBlock(
                    task="Run(uid$(BUILD_ROOT)/cloud/x/y.cpp.o)",
                    lines=("cloud/x/y.cpp:1: error: failed",),
                    paths=("cloud/x/y.cpp.o", "cloud/x/y.cpp"),
                )
            ],
            failed_build_targets=[],
            only_if_junit_failed_build=True,
        )
        == ""
    )
