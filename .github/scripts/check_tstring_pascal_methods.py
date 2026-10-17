#!/usr/bin/env python3
"""Reject PascalCase TString accessors removed from util/generic/string.h.

Newer contrib deletes TString::Size(), Data(), and Empty(). Use size(), data(),
and empty() instead. The check is limited to TString-family types so other
classes that legitimately expose Size()/Data()/Empty() are not affected.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

CPP_SUFFIXES = {".cpp", ".cc", ".cxx", ".h", ".hpp", ".hh", ".hxx"}

STRING_TYPES = (
    r"TString(?:Buf)?"
    r"|TUtf16String(?:Buf)?"
    r"|TWtringBuf"
    r"|std::basic_string<[^>]+>"
    r"|std::(?:u16|u32|w)?string"
)

BANNED_METHODS = {
    "Size": "size",
    "Data": "data",
    "Empty": "empty",
}

DECL_RE = re.compile(
    rf"\b(?:const\s+)?(?:volatile\s+)?(?:static\s+)?({STRING_TYPES})\s*"
    rf"(?:const\s*)?(?:&|\*+)?\s*(\w+)\b"
)

POINTER_DECL_RE = re.compile(
    rf"\b(?:const\s+)?(?:volatile\s+)?(?:static\s+)?({STRING_TYPES})\s*"
    rf"(?:const\s*)?\*\s*(?:const\s*)?\s*(\w+)\b"
)

INLINE_RE = re.compile(
    rf"\b({STRING_TYPES})\s*(?:\([^)]*\))?\s*\.(Size|Data|Empty)\s*\("
)

DOT_RE = re.compile(r"\b(\w+)\s*\.(Size|Data|Empty)\s*\(")
ARROW_RE = re.compile(r"\b(\w+)\s*->(Size|Data|Empty)\s*\(")


@dataclass(frozen=True)
class Violation:
    path: Path
    line_no: int
    line: str
    method: str
    replacement: str


def run_git(args: list[str]) -> str:
    return subprocess.check_output(["git", *args], text=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "files",
        nargs="*",
        help="Files to check. Defaults to staged C++ files when --cached is set.",
    )
    parser.add_argument(
        "--cached",
        action="store_true",
        help="Check only staged changes (for .githooks/pre-commit).",
    )
    parser.add_argument("--from-ref", default="")
    parser.add_argument("--to-ref", default="HEAD")
    return parser.parse_args()


def is_cpp_file(path: Path) -> bool:
    return path.suffix in CPP_SUFFIXES


def list_cached_cpp_files() -> list[Path]:
    output = run_git(["diff", "--cached", "--name-only", "--diff-filter=ACMR"])
    return [Path(line) for line in output.splitlines() if is_cpp_file(Path(line))]


def list_changed_cpp_files(diff_selector: str) -> list[Path]:
    if diff_selector == "--cached":
        cmd = ["diff", "--cached", "--name-only", "--diff-filter=ACMR"]
    else:
        cmd = ["diff", "--name-only", "--diff-filter=ACMR", diff_selector]
    output = run_git(cmd)
    return [Path(line) for line in output.splitlines() if is_cpp_file(Path(line))]


def diff_range(args: argparse.Namespace) -> str:
    if args.from_ref:
        return f"{args.from_ref}...{args.to_ref}"
    if args.cached:
        return "--cached"
    try:
        merge_base = run_git(["merge-base", "origin/main", "HEAD"]).strip()
        if merge_base:
            return f"{merge_base}...HEAD"
    except subprocess.CalledProcessError:
        pass
    return "--cached"


def added_lines(path: Path, diff_selector: str) -> list[tuple[int, str]]:
    try:
        if diff_selector == "--cached":
            diff = run_git(["diff", "--cached", "-U0", "--", str(path)])
        else:
            diff = run_git(["diff", "-U0", diff_selector, "--", str(path)])
    except subprocess.CalledProcessError:
        return []

    added: list[tuple[int, str]] = []
    current_line = 0
    for raw_line in diff.splitlines():
        if raw_line.startswith("@@"):
            match = re.search(r"\+(\d+)", raw_line)
            if match:
                current_line = int(match.group(1))
            continue
        if raw_line.startswith("+++") or raw_line.startswith("---"):
            continue
        if raw_line.startswith("+") and not raw_line.startswith("+++"):
            added.append((current_line, raw_line[1:]))
            current_line += 1
        elif raw_line.startswith("-") and not raw_line.startswith("---"):
            continue
        elif raw_line.startswith(" "):
            current_line += 1
    return added


def collect_string_identifiers(content: str) -> tuple[set[str], set[str]]:
    values: set[str] = set()
    pointers: set[str] = set()
    for match in DECL_RE.finditer(content):
        values.add(match.group(2))
    for match in POINTER_DECL_RE.finditer(content):
        pointers.add(match.group(2))
    return values, pointers


def find_violations(path: Path, content: str, added: list[tuple[int, str]]) -> list[Violation]:
    values, pointers = collect_string_identifiers(content)
    violations: list[Violation] = []

    for line_no, line in added:
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue

        for match in INLINE_RE.finditer(line):
            method = match.group(2)
            violations.append(
                Violation(path, line_no, line, method, BANNED_METHODS[method])
            )
            continue

        for match in DOT_RE.finditer(line):
            ident, method = match.group(1), match.group(2)
            if ident in values:
                violations.append(
                    Violation(path, line_no, line, method, BANNED_METHODS[method])
                )

        for match in ARROW_RE.finditer(line):
            ident, method = match.group(1), match.group(2)
            if ident in pointers:
                violations.append(
                    Violation(path, line_no, line, method, BANNED_METHODS[method])
                )

    return violations


def check_file(path: Path, diff_selector: str) -> list[Violation]:
    if not path.is_file():
        return []
    content = path.read_text(encoding="utf-8", errors="replace")
    return find_violations(path, content, added_lines(path, diff_selector))


def main() -> int:
    args = parse_args()
    diff_selector = diff_range(args)

    if args.files:
        files = [Path(f) for f in args.files if is_cpp_file(Path(f))]
    elif args.cached:
        files = list_cached_cpp_files()
    else:
        files = list_changed_cpp_files(diff_selector)

    violations: list[Violation] = []
    for path in files:
        violations.extend(check_file(path, diff_selector))

    if not violations:
        return 0

    print(
        "Found PascalCase TString accessors. "
        "Use lowercase size()/data()/empty() from util/generic/string.h:",
        file=sys.stderr,
    )
    for violation in violations:
        print(
            f"{violation.path}:{violation.line_no}: "
            f"use .{violation.replacement}() instead of .{violation.method}()",
            file=sys.stderr,
        )
        print(f"  {violation.line.rstrip()}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
