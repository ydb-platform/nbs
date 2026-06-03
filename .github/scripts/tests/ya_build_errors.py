#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
YA_MARKUP_RE = re.compile(r"\[\[(?:imp|rst|bad|good|warn|unimp)\]\]")
LOG_RECORD_RE = re.compile(r"^\d{4}-\d{2}-\d{2} .* \([^)]+\) \[[^]]+\] ")
FAILED_TASK_RE = re.compile(
    r"Task (?P<task>Run\(.*\)) failed with (?P<exit_code>\d+) exit code: ?(?P<tail>.*)$"
)
DEFAULT_MAX_CONFIGURE_ERRORS = 5
DEFAULT_MAX_COMPILER_BLOCKS = 2
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
    truncated: bool = False


def clean_log_text(text: str) -> str:
    text = ANSI_RE.sub("", text)
    text = YA_MARKUP_RE.sub("", text)
    return text.replace("\r\n", "\n").replace("\r", "\n")


def normalize_paths(text: str) -> str:
    replacements = (
        "/actions-runner/_work/nbs/nbs/",
        "/home/librarian/nbs/",
        "$(SOURCE_ROOT)/",
    )
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

        lines = _compact_lines(current_lines)
        if lines:
            truncated = len(lines) > max_lines_per_block
            if truncated:
                lines = lines[:max_lines_per_block]
                lines.append(
                    "... truncated; see ya_log.txt for the full compiler output"
                )
            blocks.append(
                CompilerErrorBlock(
                    task=normalize_paths(clean_log_text(current_task)),
                    lines=tuple(lines),
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


def render_markdown(
    *,
    configure_errors: list[ConfigureError],
    compiler_blocks: list[CompilerErrorBlock],
    ya_make_output_url: str = "",
    ya_log_url: str = "",
    evlog_url: str = "",
) -> str:
    lines: list[str] = []

    if configure_errors:
        lines.extend(["Configure errors:", "```text"])
        for error in configure_errors:
            location = error.where or "unknown location"
            suffix = f" ({error.sub})" if error.sub else ""
            lines.append(f"- {location}{suffix}")
            lines.extend(f"  {line}" for line in error.message.splitlines())
        lines.extend(["```", ""])

    if compiler_blocks:
        lines.extend(["Compiler errors:", "```text"])
        for idx, block in enumerate(compiler_blocks):
            if idx:
                lines.append("")
            lines.append(block.task)
            lines.extend(block.lines)
        lines.extend(["```", ""])

    links: list[str] = []
    if ya_make_output_url:
        links.append(f"[ya_make_output.txt]({ya_make_output_url})")
    if ya_log_url:
        links.append(f"[ya_log.txt]({ya_log_url})")
    if evlog_url:
        links.append(f"[ya_evlog.jsonl]({evlog_url})")
    if links:
        lines.append("Full logs: " + ", ".join(links))

    return "\n".join(lines).strip()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evlog", type=Path)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--ya-make-output-url", default="")
    parser.add_argument("--ya-log-url", default="")
    parser.add_argument("--evlog-url", default="")
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
    print(
        render_markdown(
            configure_errors=collect_configure_errors(
                args.evlog,
                limit=args.max_configure_errors,
            ),
            compiler_blocks=collect_compiler_error_blocks(
                args.log,
                max_blocks=args.max_compiler_blocks,
                max_lines_per_block=args.max_block_lines,
            ),
            ya_make_output_url=args.ya_make_output_url,
            ya_log_url=args.ya_log_url,
            evlog_url=args.evlog_url,
        )
    )


if __name__ == "__main__":
    main()
