#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import deque
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

DEFAULT_CLOUD_ROOTS = (
    "cloud/blockstore",
    "cloud/disk_manager",
    "cloud/filestore",
    "cloud/storage",
    "cloud/tasks",
)


Graph = Mapping[str, Mapping[str, list[str]]]


def _normalize_path(value: str) -> str:
    value = value.strip().strip("/")
    if value.endswith("/ya.make"):
        value = value[: -len("/ya.make")]
    return value


def _load_ignores(paths: Sequence[Path], inline_ignores: Sequence[str]) -> set[str]:
    ignores = {_normalize_path(value) for value in inline_ignores if value.strip()}
    for path in paths:
        for line in path.read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                ignores.add(_normalize_path(line))
    return ignores


def _run_ya_dump(ya_bin: str, roots: Sequence[str]) -> Graph:
    command = [
        ya_bin,
        "dump",
        "dir-graph",
        "--noya-tc",
        "-t",
        "--split",
        *roots,
    ]
    print(f"++ {' '.join(command)}", file=sys.stderr)
    result = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="")
    if result.returncode != 0:
        print(result.stdout, file=sys.stderr, end="")
        raise SystemExit(result.returncode)
    return json.loads(result.stdout)


def _load_graph(path: Path | None, ya_bin: str, roots: Sequence[str]) -> Graph:
    if path is not None:
        return json.loads(path.read_text())
    return _run_ya_dump(ya_bin, roots)


def _write_graph(path: Path | None, graph: Graph) -> None:
    if path is None:
        return
    path.write_text(json.dumps(graph, indent=2, sort_keys=True) + "\n")


def _closure(
    graph: Graph,
    starts: Iterable[str],
    selector: Callable[[Mapping[str, list[str]]], Iterable[str] | None],
    scopes: Sequence[str],
) -> set[str]:
    seen = {_normalize_path(start) for start in starts}
    queue = deque(seen)
    scope_prefixes = tuple(f"{_normalize_path(scope)}/" for scope in scopes)
    scope_roots = {_normalize_path(scope) for scope in scopes}

    while queue:
        current = queue.pop()
        for next_dir in selector(graph.get(current, {})) or ():
            next_dir = _normalize_path(next_dir)
            if next_dir in seen:
                continue
            if next_dir not in scope_roots and not next_dir.startswith(scope_prefixes):
                continue
            seen.add(next_dir)
            queue.append(next_dir)
    return seen


def _reachable_dirs(
    graph: Graph,
    roots: Sequence[str],
    scopes: Sequence[str],
    mode: str,
) -> set[str]:
    recurse_dirs = _closure(graph, roots, lambda node: node.get("RECURSE"), scopes)
    if mode == "recurse":
        return recurse_dirs
    if mode == "ya-style":
        return _closure(graph, recurse_dirs, lambda node: node.get("INCLUDE"), scopes)
    raise ValueError(f"unsupported mode: {mode}")


def _all_yamake_dirs(scopes: Sequence[str]) -> set[str]:
    result: set[str] = set()
    for scope in scopes:
        scope_path = Path(scope)
        if scope_path.exists():
            result.update(
                _normalize_path(str(path.parent))
                for path in scope_path.rglob("ya.make")
                if path.is_file()
            )
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find ya.make files that are not reachable from selected roots in "
            "ya dump dir-graph --split output."
        )
    )
    parser.add_argument(
        "--root",
        action="append",
        dest="roots",
        default=[],
        help=(
            "Root directory passed to ya dump dir-graph. May be repeated. "
            "Defaults to cloud component roots."
        ),
    )
    parser.add_argument(
        "--scope",
        action="append",
        dest="scopes",
        default=[],
        help="Directory whose ya.make files should be checked. May be repeated. Defaults to cloud.",
    )
    parser.add_argument("--ya-bin", default="./ya", help="Path to ya binary/script.")
    parser.add_argument(
        "--graph-json",
        type=Path,
        help="Use an existing ya dump dir-graph --split JSON file instead of running ya.",
    )
    parser.add_argument(
        "--save-graph-json",
        type=Path,
        help="Write the generated or loaded graph JSON to this path.",
    )
    parser.add_argument(
        "--mode",
        choices=("ya-style", "recurse"),
        default="ya-style",
        help=(
            "ya-style follows RECURSE from roots, then INCLUDE deps from those dirs. "
            "recurse follows only RECURSE edges."
        ),
    )
    parser.add_argument(
        "--ignore-file",
        action="append",
        type=Path,
        default=[],
        help="File with ya.make paths or directories to ignore. # comments are supported.",
    )
    parser.add_argument(
        "--ignore",
        action="append",
        default=[],
        help="ya.make path or directory to ignore. May be repeated.",
    )
    parser.add_argument(
        "--print-reachable-count",
        action="store_true",
        help="Print graph and reachable counts before missing paths.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    roots = tuple(_normalize_path(root) for root in (args.roots or DEFAULT_CLOUD_ROOTS))
    scopes = tuple(_normalize_path(scope) for scope in (args.scopes or ("cloud",)))
    ignores = _load_ignores(args.ignore_file, args.ignore)

    graph = _load_graph(args.graph_json, args.ya_bin, roots)
    _write_graph(args.save_graph_json, graph)

    reachable = _reachable_dirs(graph, roots, scopes, args.mode)
    all_yamakes = _all_yamake_dirs(scopes)
    missing = sorted(path for path in all_yamakes - reachable if path not in ignores)

    if args.print_reachable_count:
        print(f"graph_nodes={len(graph)}", file=sys.stderr)
        print(f"roots={len(roots)}", file=sys.stderr)
        print(f"scopes={len(scopes)}", file=sys.stderr)
        print(f"all_yamakes={len(all_yamakes)}", file=sys.stderr)
        print(f"reachable_dirs={len(reachable)}", file=sys.stderr)
        print(f"ignored={len(ignores)}", file=sys.stderr)
        print(f"missing={len(missing)}", file=sys.stderr)

    for path in missing:
        print(f"{path}/ya.make")

    return 1 if missing else 0


if __name__ == "__main__":
    os.environ.setdefault("YA_CACHE_DIR", "/tmp/ya-cache")
    raise SystemExit(main())
