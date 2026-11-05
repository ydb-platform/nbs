#!/usr/bin/env python3
"""Warm ya's native caches using resource-only graphs, without running build commands."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Iterable

PATTERN = re.compile(r"\$\((.*?)\)")
COMMAND_FIELDS = ("cmd_args", "cwd", "env", "stdout", "stderr")
REMOTE_SCHEMES = {"sbr", "http", "https", "docker"}


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode()).hexdigest()


def patterns(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield from PATTERN.findall(value)
    elif isinstance(value, dict):
        for child in value.values():
            yield from patterns(child)
    elif isinstance(value, list):
        for child in value:
            yield from patterns(child)


def reachable_nodes(graph: dict) -> Iterable[dict]:
    if not all(key in graph for key in ("conf", "graph", "result")):
        raise ValueError(
            "Expected a full ya make JSON graph with conf, graph and result"
        )
    nodes = {node["uid"]: node for node in graph["graph"]}
    if len(nodes) != len(graph["graph"]):
        raise ValueError("Duplicate node UIDs in graph")
    pending = list(graph["result"])
    seen = set()
    while pending:
        uid = pending.pop()
        if uid in seen:
            continue
        if uid not in nodes:
            raise ValueError(f"Missing graph node: {uid}")
        seen.add(uid)
        node = nodes[uid]
        yield node
        pending.extend(node.get("deps", []))


def remote_tool(resource: dict) -> bool:
    variants = resource.get("resources", [resource])
    if not variants:
        raise ValueError(f"Empty tool resource: {resource.get('pattern')}")
    remote = False
    for variant in variants:
        uri = variant.get("resource", "")
        scheme = uri.split(":", 1)[0]
        if scheme in REMOTE_SCHEMES:
            remote = True
        elif scheme not in {"file", "base64"}:
            raise ValueError(f"Unsupported tool resource URI: {uri}")
    return remote


def noop_node(uid: str, arguments: list[str], resources: list[dict]) -> dict:
    return {
        "uid": uid,
        "inputs": [],
        "outputs": [],
        "deps": [],
        # Always revisit resource preparation: a cached no-op must not mask eviction.
        "cache": False,
        "kv": {"p": "PREFETCH", "pc": "cyan"},
        "resources": resources,
        "cmds": [{"cmd_args": ["/bin/true", *arguments], "cwd": "$(BUILD_ROOT)"}],
    }


def make_prefetch_graph(graphs: Iterable[dict]) -> tuple[dict, dict]:
    tools: dict[str, dict] = {}
    resources: dict[str, dict] = {}
    visited = 0
    local_tools = set()
    for graph in graphs:
        definitions = {r["pattern"]: r for r in graph["conf"].get("resources", [])}
        used_patterns = set()
        for node in reachable_nodes(graph):
            visited += 1
            for value in (
                node.get("inputs", []),
                node.get("cmds", []),
                {key: node.get(key) for key in COMMAND_FIELDS},
            ):
                used_patterns.update(patterns(value))
            for resource in node.get("resources", []):
                uri = resource.get("uri", "")
                if uri.split(":", 1)[0] not in REMOTE_SCHEMES | {"ext"}:
                    raise ValueError(f"Unsupported node resource URI: {uri}")
                if uri in resources and resources[uri] != resource:
                    raise ValueError(f"Conflicting descriptions for resource {uri}")
                resources[uri] = copy.deepcopy(resource)

        for name in sorted(used_patterns & definitions.keys()):
            resource = definitions[name]
            if not remote_tool(resource):
                local_tools.add(name)
                continue
            description = {k: v for k, v in resource.items() if k != "pattern"}
            # Separate identical pattern names with different definitions across graphs.
            key = digest(description)
            tools[key] = {"pattern": f"YA_PREFETCH_{key}", **copy.deepcopy(description)}

    nodes = [
        noop_node(f"ya-prefetch-tool-{key}", [f"$({tool['pattern']})"], [])
        for key, tool in sorted(tools.items())
    ]
    nodes.extend(
        noop_node(f"ya-prefetch-resource-{digest(resource)}", [], [resource])
        for _, resource in sorted(resources.items())
    )
    # Do not copy original commands, outputs, dependency edges or execution context.
    plan = {
        "conf": {"resources": [tool for _, tool in sorted(tools.items())]},
        "graph": nodes,
        "result": [node["uid"] for node in nodes],
    }
    summary = {
        "reachable_nodes": visited,
        "tool_requirements": len(tools),
        "resource_requirements": len(resources),
        "local_tool_patterns_skipped": sorted(local_tools),
    }
    return plan, summary


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def run(command: list[str], repo: Path, env: dict[str, str]) -> None:
    print(f"++ {shlex.join(command)}", file=sys.stderr, flush=True)
    subprocess.run(command, cwd=repo, env=env, check=True)


def validate_make_args(arguments: list[str]) -> None:
    for argument in arguments:
        option = argument.split("=", 1)[0]
        if argument == "--" or option in {
            "--dist",
            "--build-custom-json",
            "--custom-context",
            "--dump-graph",
            "--dump-json-graph",
            "--dump-graph-to-file",
            "--threads",
            "-G",
        }:
            raise ValueError(
                f"The prefetcher controls {option}; omit it from ya arguments"
            )
        if argument.startswith("-j"):
            raise ValueError("Use --jobs before --; graph generation always uses -j0")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root", type=Path, default=Path(__file__).resolve().parents[3]
    )
    parser.add_argument("--ya-bin", type=Path, help="Default: <repo-root>/ya")
    parser.add_argument(
        "--cache-dir", type=Path, help="Set YA_CACHE_DIR for both phases"
    )
    parser.add_argument(
        "--jobs", type=int, default=8, help="Concurrent prefetch workers (default: 8)"
    )
    parser.add_argument(
        "--ya-arg",
        action="append",
        default=[],
        help="Common ya option, e.g. --ya-arg=--cache-size=100G",
    )
    parser.add_argument(
        "--graph-json",
        action="append",
        type=Path,
        default=[],
        help="Reuse a full graph; repeat to warm a union",
    )
    parser.add_argument("--save-graph", type=Path, help="Save the full generated graph")
    parser.add_argument(
        "--save-plan", type=Path, help="Save the resource-only graph for inspection"
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Generate the plan but do not fetch its resources",
    )
    parser.add_argument(
        "make_args",
        nargs=argparse.REMAINDER,
        help="After --: ya make flags and targets",
    )
    args = parser.parse_args(argv)
    if args.make_args[:1] == ["--"]:
        args.make_args.pop(0)
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    if args.graph_json and (args.make_args or args.save_graph):
        parser.error(
            "--graph-json cannot be combined with make arguments or --save-graph"
        )
    if not args.graph_json and not args.make_args:
        parser.error("Specify ya make flags and targets after --, or use --graph-json")
    if args.plan_only and not args.save_plan:
        parser.error("--plan-only requires --save-plan")
    try:
        validate_make_args([*args.ya_arg, *args.make_args])
    except ValueError as error:
        parser.error(str(error))
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo = args.repo_root.resolve()
    ya = str((args.ya_bin or repo / "ya").resolve())
    env = os.environ.copy()
    # In particular, use this checkout's mapping.conf.json and ext_mapping.conf.json.
    env["YA_SOURCE_ROOT"] = str(repo)
    if args.cache_dir:
        env["YA_CACHE_DIR"] = str(args.cache_dir.resolve())

    try:
        with tempfile.TemporaryDirectory(prefix="ya-prefetch-") as tmp:
            work = Path(tmp)
            paths = args.graph_json
            if not paths:
                graph_path = work / "full-graph.json"
                run(
                    [
                        ya,
                        "make",
                        *args.ya_arg,
                        *args.make_args,
                        "-j0",
                        "--no-src-links",
                        f"--output={work / 'output'}",
                        "--dump-json-graph",
                        f"--dump-graph-to-file={graph_path}",
                    ],
                    repo,
                    env,
                )
                if args.save_graph:
                    args.save_graph.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(graph_path, args.save_graph)
                paths = [graph_path]
            plan, summary = make_prefetch_graph(
                json.loads(path.read_text()) for path in paths
            )
            print(json.dumps(summary, indent=2), file=sys.stderr)
            plan_path = work / "prefetch.json"
            write_json(plan_path, plan)
            if args.save_plan:
                write_json(args.save_plan, plan)
            if args.plan_only or not plan["result"]:
                return 0
            if not Path("/bin/true").is_file():
                raise ValueError(
                    "Prefetch execution requires /bin/true (Linux/macOS host)"
                )
            run(
                [
                    ya,
                    "make",
                    *args.ya_arg,
                    f"--build-custom-json={plan_path}",
                    f"-j{args.jobs}",
                    "--no-src-links",
                    f"--output={work / 'output'}",
                ],
                repo,
                env,
            )
        return 0
    except subprocess.CalledProcessError as error:
        return error.returncode if error.returncode > 0 else 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Prefetch failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
