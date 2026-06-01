#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path
from xml.etree import ElementTree as ET

from .generate_summary import TestResult, render_testlist_html
from .junit_utils import add_junit_log_property

FLAKE8_RE = re.compile(
    r"^(?P<path>.*?):(?P<line>\d+):(?P<column>\d+): (?P<code>[A-Z]+\d+) (?P<message>.*)$"
)
SHELLCHECK_RE = re.compile(
    r"^(?P<path>.*?):(?P<line>\d+):(?P<column>\d+): (?P<severity>[a-z]+): "
    r"(?P<message>.*?) \[(?P<code>SC\d+)\]$"
)
SHFMT_HUNK_RE = re.compile(
    r"^@@ -(?P<old_line>\d+)(?:,\d+)? \+(?P<new_line>\d+)(?:,\d+)? @@"
)


def _read_log(path: Path) -> str:
    return path.read_text(errors="replace")


def _limited_failure_text(text: str) -> str:
    return text[:8192]


def _make_case(
    *,
    classname: str,
    name: str,
    elapsed: float,
    log_url: str,
    failure: str | None = None,
    error: str | None = None,
) -> ET.Element:
    case = ET.Element(
        "testcase",
        {"classname": classname, "name": name, "time": f"{elapsed:.3f}"},
    )
    if log_url:
        add_junit_log_property(case, log_url)
    if failure is not None:
        failure_node = ET.SubElement(case, "failure", {"type": "failure"})
        failure_node.text = _limited_failure_text(failure)
    if error is not None:
        error_node = ET.SubElement(case, "error", {"type": "error"})
        error_node.text = _limited_failure_text(error)
    return case


def _case_name_from_path(path: str) -> tuple[str, str]:
    p = Path(path)
    parent = str(p.parent)
    if parent == ".":
        parent = "black"
    return parent, p.name


def parse_flake8(log_text: str, elapsed: float, log_url: str) -> list[ET.Element]:
    cases = []
    for line in log_text.splitlines():
        match = FLAKE8_RE.match(line)
        if match is None:
            continue
        classname = match.group("path")
        name = f"{match.group('line')}:{match.group('column')} {match.group('code')}"
        message = match.group("message")
        cases.append(
            _make_case(
                classname=classname,
                name=name,
                elapsed=elapsed,
                log_url=log_url,
                failure=f"{line}\n\n{message}",
            )
        )
    return cases


def parse_black(log_text: str, elapsed: float, log_url: str) -> list[ET.Element]:
    cases = []
    for line in log_text.splitlines():
        if line.startswith("would reformat "):
            path = line.removeprefix("would reformat ").strip()
            classname, name = _case_name_from_path(path)
            cases.append(
                _make_case(
                    classname=classname,
                    name=name,
                    elapsed=elapsed,
                    log_url=log_url,
                    failure=line,
                )
            )
            continue

        if line.startswith("error: cannot format "):
            path = line.removeprefix("error: cannot format ").split(":", 1)[0].strip()
            classname, name = _case_name_from_path(path)
            cases.append(
                _make_case(
                    classname=classname,
                    name=name,
                    elapsed=elapsed,
                    log_url=log_url,
                    error=line,
                )
            )
    return cases


def parse_shellcheck(log_text: str, elapsed: float, log_url: str) -> list[ET.Element]:
    cases = []
    for line in log_text.splitlines():
        match = SHELLCHECK_RE.match(line)
        if match is None:
            continue
        classname = match.group("path")
        name = f"{match.group('line')}:{match.group('column')} {match.group('code')}"
        message = f"{match.group('severity')}: {match.group('message')}"
        cases.append(
            _make_case(
                classname=classname,
                name=name,
                elapsed=elapsed,
                log_url=log_url,
                failure=f"{line}\n\n{message}",
            )
        )
    return cases


def parse_shfmt(log_text: str, elapsed: float, log_url: str) -> list[ET.Element]:
    cases = []
    current_path = ""
    current_line = ""
    current_block: list[str] = []
    listed_paths: list[str] = []

    def add_current() -> None:
        if not current_path:
            return
        classname, name = _case_name_from_path(current_path)
        if current_line:
            name = f"{name}:{current_line}"
        cases.append(
            _make_case(
                classname=classname,
                name=name,
                elapsed=elapsed,
                log_url=log_url,
                failure="\n".join(current_block),
            )
        )

    for line in log_text.splitlines():
        if line.startswith("--- "):
            add_current()
            current_line = ""
            current_block = [line]
            current_path = line.removeprefix("--- ").split("\t", 1)[0].strip()
            current_path = current_path.removesuffix(".orig")
            continue
        hunk_match = SHFMT_HUNK_RE.match(line)
        if hunk_match is not None and not current_line:
            current_line = hunk_match.group("old_line")
        if current_block:
            current_block.append(line)
        elif (
            line.strip() and not line.startswith("+++ ") and not line.startswith("@@ ")
        ):
            listed_paths.append(line.strip())

    add_current()
    for path in listed_paths:
        classname, name = _case_name_from_path(path)
        cases.append(
            _make_case(
                classname=classname,
                name=name,
                elapsed=elapsed,
                log_url=log_url,
                failure=f"{path} is not shfmt-formatted",
            )
        )
    return cases


def build_cases(
    *,
    tool: str,
    title: str,
    command: str,
    exit_code: int,
    elapsed: float,
    log_text: str,
    log_url: str,
) -> list[ET.Element]:
    if tool == "flake8":
        cases = parse_flake8(log_text, elapsed, log_url)
    elif tool == "black":
        cases = parse_black(log_text, elapsed, log_url)
    elif tool == "shellcheck":
        cases = parse_shellcheck(log_text, elapsed, log_url)
    elif tool == "shfmt":
        cases = parse_shfmt(log_text, elapsed, log_url)
    else:
        cases = []

    if cases:
        return cases

    if exit_code == 0:
        return [
            _make_case(
                classname=tool,
                name=title,
                elapsed=elapsed,
                log_url=log_url,
            )
        ]

    return [
        _make_case(
            classname=tool,
            name=title,
            elapsed=elapsed,
            log_url=log_url,
            failure=(
                f"Command failed with exit code {exit_code}:\n{command}\n\n{log_text}"
                if log_text
                else f"Command failed with exit code {exit_code}:\n{command}"
            ),
        )
    ]


def write_junit(path: Path, cases: list[ET.Element]) -> None:
    failures = sum(1 for case in cases if case.find("failure") is not None)
    errors = sum(1 for case in cases if case.find("error") is not None)
    tests = len(cases)
    elapsed = sum(float(case.get("time") or 0.0) for case in cases)

    suite = ET.Element(
        "testsuite",
        {
            "name": "github-actions-lint",
            "tests": str(tests),
            "failures": str(failures),
            "errors": str(errors),
            "time": f"{elapsed:.3f}",
        },
    )
    suite.extend(cases)
    root = ET.Element(
        "testsuites",
        {
            "tests": str(tests),
            "failures": str(failures),
            "errors": str(errors),
            "time": f"{elapsed:.3f}",
        },
    )
    root.append(suite)
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def write_html(path: Path, cases: list[ET.Element], summary_url_prefix: str) -> None:
    results = [TestResult.from_junit(case) for case in cases]
    render_testlist_html(
        sorted(results, key=lambda result: result.full_name),
        str(path),
        summary_url=summary_url_prefix,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tool",
        choices=("flake8", "black", "shellcheck", "shfmt", "command"),
        required=True,
    )
    parser.add_argument("--title", required=True)
    parser.add_argument("--command", required=True)
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--elapsed", type=float, required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--log-url", default="")
    parser.add_argument("--junit", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--summary-url-prefix", default="")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    log_text = _read_log(args.log)
    cases = build_cases(
        tool=args.tool,
        title=args.title,
        command=args.command,
        exit_code=args.exit_code,
        elapsed=args.elapsed,
        log_text=log_text,
        log_url=args.log_url,
    )

    args.junit.parent.mkdir(parents=True, exist_ok=True)
    args.html.parent.mkdir(parents=True, exist_ok=True)
    write_junit(args.junit, cases)
    write_html(args.html, cases, args.summary_url_prefix)


if __name__ == "__main__":
    main()
