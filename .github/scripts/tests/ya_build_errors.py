#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
YA_MARKUP_RE = re.compile(r"\[\[(?:imp|rst|bad|good|warn|unimp)\]\]")
LOG_RECORD_RE = re.compile(r"^\d{4}-\d{2}-\d{2} .* \([^)]+\) \[[^]]+\] ")
FAILED_TASK_RE = re.compile(
    r"Task (?P<task>Run\(.*\)) failed with (?P<exit_code>\d+) exit code: ?(?P<tail>.*)$"
)
BUILD_ROOT_PATH_RE = re.compile(r"\$\(BUILD_ROOT\)/(?P<path>[^)\s]+)")
PATH_TOKEN_RE = re.compile(r"(?P<path>(?:[A-Za-z0-9_.+@%-]+/)+[A-Za-z0-9_.+@%=-]+)")
ANCHOR_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")
DEFAULT_MAX_CONFIGURE_ERRORS = 5
DEFAULT_MAX_COMPILER_BLOCKS = 20
DEFAULT_MAX_BLOCK_LINES = 24


@dataclass(frozen=True)
class ConfigureError:
    where: str
    sub: str
    message: str


@dataclass(frozen=True)
class CompilerErrorBlock:
    task: str
    lines: tuple[str, ...]
    paths: tuple[str, ...] = ()
    truncated: bool = False


@dataclass(frozen=True)
class FailedBuildTarget:
    name: str
    testcase: str


def clean_log_text(text: str) -> str:
    text = ANSI_RE.sub("", text)
    text = YA_MARKUP_RE.sub("", text)
    return text.replace("\r\n", "\n").replace("\r", "\n")


def anchor_id(value: str) -> str:
    return ANCHOR_ID_RE.sub("-", value.strip()).strip("-") or "target"


def normalize_paths(text: str) -> str:
    replacements = ["$(SOURCE_ROOT)/"]
    for path in (os.environ.get("GITHUB_WORKSPACE"), Path.cwd()):
        if not path:
            continue
        workspace = str(path).rstrip("/")
        if workspace:
            replacements.append(f"{workspace}/")

    for prefix in replacements:
        text = text.replace(prefix, "")
    return text


def _compact_lines(lines: Iterable[str]) -> list[str]:
    result: list[str] = []
    previous_blank = False
    for raw_line in lines:
        line = normalize_paths(clean_log_text(raw_line)).rstrip()
        blank = line == ""
        if blank and previous_blank:
            continue
        result.append(line)
        previous_blank = blank

    while result and result[0] == "":
        result.pop(0)
    while result and result[-1] == "":
        result.pop()
    return result


def _normalize_path(path: str) -> str:
    return normalize_paths(clean_log_text(path)).strip().strip('"')


def _dedupe_keep_order(items: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        item = _normalize_path(item)
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return tuple(result)


def _extract_paths(text: str) -> tuple[str, ...]:
    text = normalize_paths(clean_log_text(text))
    paths: list[str] = []
    paths.extend(match.group("path") for match in BUILD_ROOT_PATH_RE.finditer(text))
    paths.extend(match.group("path") for match in PATH_TOKEN_RE.finditer(text))
    return _dedupe_keep_order(paths)


def _strip_absolute_prefixes_before_roots(text: str, roots: Iterable[str]) -> str:
    roots = tuple(root for root in roots if root)
    if not roots:
        return text

    pattern = re.compile(
        rf"(?<![\w./-])(?:/[^\s:()]+/)+(?=(?:{'|'.join(map(re.escape, roots))})/)"
    )
    return pattern.sub("", text)


def _target_match_prefixes(target: str) -> tuple[str, ...]:
    target = target.strip().strip("/")
    if not target:
        return ()

    prefixes = [target]
    for suffix in ("/ut", "/tests", "/test"):
        if target.endswith(suffix):
            prefixes.append(target[: -len(suffix)])

    if "/ut/" in target:
        prefixes.append(target.split("/ut/", 1)[0])
    if "/tests/" in target:
        prefixes.append(target.split("/tests/", 1)[0])

    return _dedupe_keep_order(prefixes)


def block_matches_target(block: CompilerErrorBlock, target: str) -> bool:
    prefixes = _target_match_prefixes(target)
    if not prefixes:
        return False

    paths = block.paths or _extract_paths("\n".join((block.task, *block.lines)))
    for path in paths:
        path = path.strip("/")
        if any(path == prefix or path.startswith(f"{prefix}/") for prefix in prefixes):
            return True
    return False


def collect_failed_build_targets(junit_path: Path | None) -> list[FailedBuildTarget]:
    if junit_path is None or not junit_path.exists():
        return []

    try:
        root = ET.parse(junit_path).getroot()
    except ET.ParseError:
        return []

    suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
    targets: list[FailedBuildTarget] = []
    seen: set[str] = set()
    for suite in suites:
        suite_name = suite.get("name") or ""
        for testcase in suite.findall("testcase"):
            failure = testcase.find("failure")
            if failure is None:
                continue

            failure_text = failure.text or ""
            if "skipped due to a failed build" not in failure_text:
                continue

            target = suite_name
            for line in failure_text.splitlines():
                line = line.strip()
                if line.startswith("Depends on broken:"):
                    target = line.split(":", 1)[1].strip()
                    break

            target = _normalize_path(target)
            if not target or target in seen:
                continue

            seen.add(target)
            testcase_name = testcase.get("name") or ""
            targets.append(FailedBuildTarget(name=target, testcase=testcase_name))

    return targets


def collect_configure_errors(
    evlog_path: Path | None,
    *,
    limit: int = DEFAULT_MAX_CONFIGURE_ERRORS,
) -> list[ConfigureError]:
    if evlog_path is None or not evlog_path.exists():
        return []

    errors: list[ConfigureError] = []
    seen: set[ConfigureError] = set()
    with evlog_path.open(encoding="utf-8") as fp:
        for line in fp:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            value = event.get("value")
            if not isinstance(value, dict) or value.get("Type") != "Error":
                continue

            message = "\n".join(_compact_lines(value.get("Message", "").splitlines()))
            if not message:
                continue

            error = ConfigureError(
                where=normalize_paths(clean_log_text(value.get("Where", ""))).strip(),
                sub=normalize_paths(clean_log_text(value.get("Sub", ""))).strip(),
                message=message,
            )
            if error in seen:
                continue
            seen.add(error)
            errors.append(error)
            if len(errors) >= limit:
                break

    return errors


def _strip_log_prefix(line: str) -> str:
    return LOG_RECORD_RE.sub("", line, count=1)


def collect_compiler_error_blocks(
    log_path: Path | None,
    *,
    max_blocks: int = DEFAULT_MAX_COMPILER_BLOCKS,
    max_lines_per_block: int = DEFAULT_MAX_BLOCK_LINES,
) -> list[CompilerErrorBlock]:
    if log_path is None or not log_path.exists():
        return []

    blocks: list[CompilerErrorBlock] = []
    current_task = ""
    current_lines: list[str] = []

    def flush_current() -> None:
        nonlocal current_task, current_lines
        if not current_task:
            return

        task_paths = tuple(
            match.group("path") for match in BUILD_ROOT_PATH_RE.finditer(current_task)
        )
        task_roots = tuple(path.split("/", 1)[0] for path in task_paths)
        task = _strip_absolute_prefixes_before_roots(current_task, task_roots)
        raw_lines = [
            _strip_absolute_prefixes_before_roots(line, task_roots)
            for line in current_lines
        ]

        lines = _compact_lines(raw_lines)
        if lines:
            truncated = len(lines) > max_lines_per_block
            if truncated:
                lines = lines[:max_lines_per_block]
                lines.append(
                    "... truncated; see ya_log.txt for the full compiler output"
                )
            blocks.append(
                CompilerErrorBlock(
                    task=normalize_paths(clean_log_text(task)),
                    lines=tuple(lines),
                    paths=_extract_paths("\n".join((task, *lines))),
                    truncated=truncated,
                )
            )

        current_task = ""
        current_lines = []

    with log_path.open(encoding="utf-8", errors="replace") as fp:
        for raw_line in fp:
            line = clean_log_text(raw_line.rstrip("\n"))
            match = FAILED_TASK_RE.search(line)
            if match:
                flush_current()
                if len(blocks) >= max_blocks:
                    break

                tail = match.group("tail").strip()
                if tail and not re.search(r"no str?derr was provided", tail):
                    current_task = match.group("task")
                    current_lines = [_strip_log_prefix(tail)]
                elif not tail:
                    current_task = match.group("task")
                    current_lines = []
                continue

            if not current_task:
                continue

            if LOG_RECORD_RE.match(line):
                flush_current()
                if len(blocks) >= max_blocks:
                    break
                continue

            current_lines.append(line)

    if len(blocks) < max_blocks:
        flush_current()

    return blocks[:max_blocks]


def _log_links(
    *,
    ya_make_output_url: str = "",
    ya_log_url: str = "",
    evlog_url: str = "",
) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    if ya_make_output_url:
        links.append(("ya_make_output.txt", ya_make_output_url))
    if ya_log_url:
        links.append(("ya_log.txt", ya_log_url))
    if evlog_url:
        links.append(("ya_evlog.jsonl", evlog_url))
    return links


def build_report(
    *,
    configure_errors: list[ConfigureError],
    compiler_blocks: list[CompilerErrorBlock],
    failed_build_targets: list[FailedBuildTarget] | None = None,
    only_if_junit_failed_build: bool = False,
) -> dict[str, object] | None:
    failed_build_targets = failed_build_targets or []
    if only_if_junit_failed_build and not failed_build_targets:
        return None

    matched_ids: set[int] = set()
    target_sections: list[dict[str, object]] = []
    for target in failed_build_targets:
        blocks = []
        for block in compiler_blocks:
            if block_matches_target(block, target.name):
                blocks.append(block)
                matched_ids.add(id(block))
        target_sections.append({"target": target, "blocks": blocks})

    unmatched_blocks = [
        block for block in compiler_blocks if id(block) not in matched_ids
    ]

    has_content = (
        bool(configure_errors) or bool(compiler_blocks) or bool(failed_build_targets)
    )
    if not has_content:
        return None

    return {
        "configure_errors": configure_errors,
        "target_sections": target_sections,
        "unmatched_blocks": unmatched_blocks,
        "failed_build_targets": failed_build_targets,
    }


def render_html(
    *,
    configure_errors: list[ConfigureError],
    compiler_blocks: list[CompilerErrorBlock],
    failed_build_targets: list[FailedBuildTarget] | None = None,
    ya_make_output_url: str = "",
    ya_log_url: str = "",
    evlog_url: str = "",
    only_if_junit_failed_build: bool = False,
) -> str:
    report = build_report(
        configure_errors=configure_errors,
        compiler_blocks=compiler_blocks,
        failed_build_targets=failed_build_targets,
        only_if_junit_failed_build=only_if_junit_failed_build,
    )
    if report is None:
        return ""

    templates_path = Path(__file__).with_name("templates")
    env = Environment(
        loader=FileSystemLoader(str(templates_path)),
        undefined=StrictUndefined,
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["anchor_id"] = anchor_id

    return (
        env.get_template("build_errors.html")
        .render(
            **report,
            log_links=_log_links(
                ya_make_output_url=ya_make_output_url,
                ya_log_url=ya_log_url,
                evlog_url=evlog_url,
            ),
        )
        .strip()
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evlog", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--junit", type=Path)
    parser.add_argument("--ya-make-output-url", default="")
    parser.add_argument("--ya-log-url", default="")
    parser.add_argument("--evlog-url", default="")
    parser.add_argument(
        "--only-if-junit-failed-build",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--max-configure-errors",
        type=int,
        default=DEFAULT_MAX_CONFIGURE_ERRORS,
    )
    parser.add_argument(
        "--max-compiler-blocks",
        type=int,
        default=DEFAULT_MAX_COMPILER_BLOCKS,
    )
    parser.add_argument(
        "--max-block-lines",
        type=int,
        default=DEFAULT_MAX_BLOCK_LINES,
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    html = render_html(
        configure_errors=collect_configure_errors(
            args.evlog,
            limit=args.max_configure_errors,
        ),
        compiler_blocks=collect_compiler_error_blocks(
            args.log,
            max_blocks=args.max_compiler_blocks,
            max_lines_per_block=args.max_block_lines,
        ),
        failed_build_targets=collect_failed_build_targets(args.junit),
        ya_make_output_url=args.ya_make_output_url,
        ya_log_url=args.ya_log_url,
        evlog_url=args.evlog_url,
        only_if_junit_failed_build=args.only_if_junit_failed_build,
    )
    if html:
        sys.stdout.write(f"{html}\n")


if __name__ == "__main__":
    main()
