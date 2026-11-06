"""Generate and compare ya test graphs without building or running tests."""

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath


def csv_values(value):
    return [part.strip() for part in value.split(",") if part.strip()]


def target_path(value):
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts or value.startswith("-"):
        raise ValueError(f"Expected a repository-relative target, got {value!r}")
    if any(char in value for char in "\n\r\0"):
        raise ValueError(f"Invalid target: {value!r}")
    return str(path)


def read_suites(path):
    """Group split test nodes by suite; ya's cached UIDs include transitive inputs."""
    with path.open() as stream:
        graph = json.load(stream)
    if not isinstance(graph, dict) or not isinstance(graph.get("graph"), list):
        raise ValueError(f"Invalid ya graph: {path}")
    suites = {}
    for node in graph["graph"]:
        if not isinstance(node, dict) or not isinstance(node.get("uid"), str):
            raise ValueError(f"Invalid node in {path}")
        kv = node.get("kv", {})
        if not kv.get("run_test_node"):
            continue
        name = kv.get("path", "")
        if not isinstance(name, str) or "/" not in name:
            raise ValueError(f"Test node has no suite path in {path}: {node['uid']}")
        target_path(name)
        # Platform is part of a suite's identity, even if callers later add cross compilation.
        key = (name, node.get("platform", ""))
        suites.setdefault(key, set()).add(node["uid"])
    return suites


def compare_suites(base, head):
    def describe(key):
        name, platform = key
        return {
            "suite": name,
            "platform": platform,
            "target": target_path(name.rsplit("/", 1)[0]),
        }

    selected = [
        {**describe(key), "reason": "added" if key not in base else "changed"}
        for key in sorted(head)
        if base.get(key) != head[key]
    ]
    targets = sorted({suite["target"] for suite in selected})
    # Execution uses normal ya make so reports and exit codes remain available.
    # A directory may contain several suites: report the conservative expansion.
    executed = [
        describe(key) for key in sorted(head) if describe(key)["target"] in targets
    ]
    return {
        "base_suite_count": len(base),
        "head_suite_count": len(head),
        "selected_suites": selected,
        "execution_suites": executed,
        "removed_suites": [describe(key) for key in sorted(base.keys() - head.keys())],
        "targets": targets,
    }


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def resolve_commit(repo, ref):
    return git(repo, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}")


def run_logged(command, cwd, env, log):
    print(f"+ {shlex.join(map(str, command))}\n  log: {log}", flush=True)
    with log.open("w") as stream:
        result = subprocess.run(
            command, cwd=cwd, env=env, stdout=stream, stderr=subprocess.STDOUT
        )
    if result.returncode:
        # Keep failures visible in the job log without loading a potentially huge build log.
        with log.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            stream.seek(max(0, stream.tell() - 8000))
            print(stream.read().decode(errors="replace"), file=sys.stderr)
    return result.returncode


def checkout(repo, commit, env, log):
    git(repo, "checkout", "--detach", "--force", commit)
    # This is our disposable clone, never the caller's checkout. Remove files that
    # disappeared between revisions, including old submodule working directories.
    git(repo, "clean", "-ffdx")
    if (repo / ".gitmodules").exists():
        git(repo, "submodule", "sync", "--recursive")
        command = ["git", "submodule", "update", "--init", "--recursive"]
        if run_logged(command, repo, env, log):
            raise RuntimeError(f"Submodule checkout failed; see {log}")


def write_report(output, report):
    (output / "selection.json").write_text(json.dumps(report, indent=2) + "\n")
    (output / "targets.txt").write_text(
        "".join(f"{target}\n" for target in report["targets"])
    )
    lines = [
        "## Affected tests",
        "",
        f"- Workflow commit: `{report['workflow_commit'] or 'local'}`",
        f"- App head: `{report['head_commit']}`",
        f"- Requested base: `{report['base_ref']}` (`{report['base_ref_commit']}`)",
        f"- Compared base: `{report['base_commit']}` ({report['base_mode']})",
        f"- Suites: {report['base_suite_count']} before, {report['head_suite_count']} after; "
        f"{len(report['selected_suites'])} affected, {len(report['removed_suites'])} removed",
        f"- Execution: {len(report['execution_suites'])} suites in {len(report['targets'])} targets",
        f"- Status: **{report['status']}**",
        "",
        "All matching suites in each affected target run, including all of their chunks.",
        "See `selection.json`, `targets.txt`, and the graph dumps for full details.",
        "",
    ]
    lines.extend(f"- `{target}`" for target in report["targets"][:100])
    if len(report["targets"]) > 100:
        lines.append("- Further targets are listed in `targets.txt`.")
    (output / "summary.md").write_text("\n".join(lines) + "\n")


def execute(args):
    repo = args.repo.resolve()
    output = args.output.resolve()
    targets = [target_path(target) for target in csv_values(args.targets)]
    if not targets:
        raise ValueError("At least one target is required")
    head = resolve_commit(repo, args.head_ref)
    requested_base = resolve_commit(repo, args.base_ref)
    base = (
        requested_base
        if args.exact_base
        else git(repo, "merge-base", requested_base, head)
    )
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError(f"Output directory must be empty: {output}")

    with tempfile.TemporaryDirectory(prefix="nbs-affected-tests-") as temporary:
        temporary = Path(temporary)
        source = temporary / "source"
        subprocess.run(
            [
                "git",
                "clone",
                "--quiet",
                "--shared",
                "--no-checkout",
                str(repo),
                str(source),
            ],
            check=True,
        )
        # Retain the original remote for relative submodule URLs.
        remote = subprocess.run(
            ["git", "-C", str(repo), "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
        )
        if remote.returncode == 0:
            git(source, "remote", "set-url", "origin", remote.stdout.strip())
        # Pin both graph generations to the head's ya version. Keep the launcher
        # outside the app checkout so switching commits cannot replace it.
        ya = temporary / "ya"
        ya.write_bytes(
            subprocess.check_output(["git", "-C", str(repo), "show", f"{head}:ya"])
        )
        ya.chmod(0o755)
        env = {**os.environ, "YA_SOURCE_ROOT": str(source)}
        graphs = {}
        found_targets = set()
        for label, commit in (("base", base), ("head", head)):
            checkout(source, commit, env, output / f"{label}-submodules.log")
            # Newly added or removed requested targets need not exist in both revisions.
            existing = [
                target for target in targets if (source / target / "ya.make").is_file()
            ]
            found_targets.update(existing)
            graph_path = output / f"{label}-graph.json"
            if existing:
                command = [
                    str(ya),
                    "make",
                    *args.make_args,
                    "--run-all-tests",
                    "--no-src-links",
                    "--no-yt-store",
                    "--cache-tests",
                    "--no-bazel-remote-store",
                    "-j0",
                    "--dump-json-graph",
                    f"--dump-graph-to-file={graph_path}",
                    *[f"--target={target}" for target in existing],
                ]
                if run_logged(command, source, env, output / f"{label}-graph.log"):
                    raise RuntimeError(f"{label} graph generation failed")
            else:
                graph_path.write_text('{"graph": []}\n')
            graphs[label] = read_suites(graph_path)

        missing = set(targets) - found_targets
        if missing:
            raise ValueError(
                f"Targets absent from both revisions: {', '.join(sorted(missing))}"
            )
        report = {
            "workflow_commit": os.environ.get("GITHUB_SHA", ""),
            "head_commit": head,
            "base_ref": args.base_ref,
            "base_ref_commit": requested_base,
            "base_commit": base,
            "base_mode": "exact ref" if args.exact_base else "merge base",
            "requested_targets": targets,
            "make_args": args.make_args,
            **compare_suites(graphs["base"], graphs["head"]),
        }
        report["status"] = "selected" if report["targets"] else "no affected tests"
        write_report(output, report)
        print(
            f"Selected {len(report['selected_suites'])} suites in {len(report['targets'])} targets",
            flush=True,
        )
        return 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base-ref", default="origin/main")
    parser.add_argument("--head-ref", default="HEAD")
    parser.add_argument(
        "--exact-base",
        action="store_true",
        help="Compare directly to base-ref instead of its merge base",
    )
    parser.add_argument(
        "--targets",
        required=True,
        help="Comma-separated repository-relative test targets",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "make_args",
        nargs=argparse.REMAINDER,
        help="Ya build flags and test filters after -- (supplied by the test action)",
    )
    args = parser.parse_args(argv)
    if args.make_args[:1] == ["--"]:
        args.make_args = args.make_args[1:]
    return args


def main(argv=None):
    try:
        return execute(parse_args(argv))
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Affected test selection failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
