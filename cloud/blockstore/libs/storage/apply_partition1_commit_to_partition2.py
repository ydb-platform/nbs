#!/usr/bin/env python3

"""Apply a commit containing Partition 1 changes to Partition 2.

Changes under cloud/blockstore/libs/storage/partition are mechanically
transformed in the same way as replace_partition2_with_partition1.py.  All
other changes in the commit keep their original paths and contents.  The
resulting patch is applied with ``git apply --3way``.  A clean application is
staged; a conflicting application leaves conflict markers in the affected
files and unmerged index entries to resolve with the usual Git workflow.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import os
import re
import subprocess
import sys
import tempfile

from replace_partition2_with_partition1 import (
    BINARY_SUFFIXES,
    DESTINATION_REL,
    SOURCE_REL,
    destination_path,
    replacement_filenames,
    transform_text,
)


IMPLEMENTATION_MACRO = re.compile(
    rb"^\s*#\s*define\s+(BLOCKSTORE_PARTITION[A-Z0-9_]*)",
    re.MULTILINE,
)


@dataclass(frozen=True)
class TreeEntry:
    mode: str
    oid: str
    relative_path: Path


def run_git(
    repo_root: Path,
    *args: str,
    input_data: bytes | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    process = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        input=input_data,
        env=env,
        check=False,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
    )
    if check and process.returncode:
        stderr = process.stderr.decode(errors="replace").strip()
        stdout = process.stdout.decode(errors="replace").strip()
        message = stderr or stdout or f"exit code {process.returncode}"
        raise RuntimeError(f"git {' '.join(args)} failed: {message}")
    return process


def find_repo_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    process = subprocess.run(
        ["git", "-C", str(script_dir), "rev-parse", "--show-toplevel"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode:
        raise RuntimeError("the script must be run from an NBS Git checkout")
    return Path(process.stdout.strip()).resolve()


def resolve_commit(repo_root: Path, revision: str) -> str:
    process = run_git(
        repo_root,
        "rev-parse",
        "--verify",
        f"{revision}^{{commit}}",
    )
    return process.stdout.decode().strip()


def select_parent(repo_root: Path, commit: str, parent_number: int) -> str:
    process = run_git(repo_root, "rev-list", "--parents", "-n", "1", commit)
    fields = process.stdout.decode().split()
    parents = fields[1:]
    if not parents:
        raise RuntimeError("a root commit has no parent to use as a merge base")
    if parent_number > len(parents):
        raise RuntimeError(
            f"commit has {len(parents)} parent(s), not parent {parent_number}"
        )
    return parents[parent_number - 1]


def read_source_tree(repo_root: Path, revision: str) -> dict[Path, TreeEntry]:
    process = run_git(
        repo_root,
        "ls-tree",
        "-r",
        "-z",
        "--full-tree",
        revision,
        "--",
        SOURCE_REL.as_posix(),
    )
    prefix = os.fsencode(SOURCE_REL.as_posix() + "/")
    result: dict[Path, TreeEntry] = {}

    for record in process.stdout.split(b"\0"):
        if not record:
            continue
        metadata, separator, path_bytes = record.partition(b"\t")
        if not separator or not path_bytes.startswith(prefix):
            raise RuntimeError("Git returned an unexpected tree entry")
        mode, object_type, oid = metadata.decode().split()
        if object_type != "blob":
            raise RuntimeError(
                f"unsupported {object_type} entry: {os.fsdecode(path_bytes)}"
            )

        relative_path = Path(os.fsdecode(path_bytes[len(prefix) :]))
        if relative_path in result:
            raise RuntimeError(f"duplicate source path: {relative_path}")
        result[relative_path] = TreeEntry(mode, oid, relative_path)

    return result


def changed_commit_paths(
    repo_root: Path,
    parent: str,
    commit: str,
) -> list[str]:
    process = run_git(
        repo_root,
        "diff",
        "--name-only",
        "-z",
        "--no-renames",
        parent,
        commit,
    )
    return [os.fsdecode(path) for path in process.stdout.split(b"\0") if path]


def read_selected_tree_entries(
    repo_root: Path,
    revision: str,
    paths: list[str],
) -> dict[Path, TreeEntry]:
    result: dict[Path, TreeEntry] = {}
    batch_size = 256

    for start in range(0, len(paths), batch_size):
        batch = paths[start : start + batch_size]
        process = run_git(
            repo_root,
            "ls-tree",
            "-r",
            "-z",
            "--full-tree",
            revision,
            "--",
            *(f":(literal){path}" for path in batch),
        )
        for record in process.stdout.split(b"\0"):
            if not record:
                continue
            metadata, separator, path_bytes = record.partition(b"\t")
            if not separator:
                raise RuntimeError("Git returned an unexpected tree entry")
            mode, object_type, oid = metadata.decode().split()
            path = Path(os.fsdecode(path_bytes))
            if object_type != "blob":
                raise RuntimeError(
                    f"unsupported {object_type} entry: {path.as_posix()}"
                )
            if path in result:
                raise RuntimeError(f"duplicate commit path: {path.as_posix()}")
            result[path] = TreeEntry(mode, oid, path)

    return result


def read_blob(repo_root: Path, oid: str) -> bytes:
    return run_git(repo_root, "cat-file", "blob", oid).stdout


def find_implementation_macros(
    repo_root: Path,
    trees: tuple[dict[Path, TreeEntry], dict[Path, TreeEntry]],
) -> set[str]:
    result: set[str] = set()
    seen: set[str] = set()
    for tree in trees:
        for entry in tree.values():
            if entry.oid in seen or entry.relative_path.suffix in BINARY_SUFFIXES:
                continue
            seen.add(entry.oid)
            content = read_blob(repo_root, entry.oid)
            result.update(
                match.decode() for match in IMPLEMENTATION_MACRO.findall(content)
            )
    return result


def write_blob(repo_root: Path, content: bytes) -> str:
    process = run_git(repo_root, "hash-object", "-w", "--stdin", input_data=content)
    return process.stdout.decode().strip()


def transform_blob(
    repo_root: Path,
    entry: TreeEntry,
    filename_replacements: dict[str, str],
    macros: set[str],
) -> str:
    content = read_blob(repo_root, entry.oid)
    if entry.relative_path.suffix not in BINARY_SUFFIXES:
        try:
            text = content.decode()
        except UnicodeDecodeError as error:
            raise RuntimeError(
                f"non-UTF-8 source file is not marked binary: "
                f"{entry.relative_path}"
            ) from error
        content = transform_text(text, filename_replacements, macros).encode()
    return write_blob(repo_root, content)


def write_transformed_tree(
    repo_root: Path,
    source_tree: dict[Path, TreeEntry],
    changed_source_paths: set[Path],
    untransformed_tree: dict[Path, TreeEntry],
    filename_replacements: dict[str, str],
    macros: set[str],
    temporary_directory: Path,
    name: str,
) -> str:
    index_entries: list[bytes] = []
    destination_paths: set[Path] = set()

    for relative_path in sorted(
        changed_source_paths,
        key=lambda path: path.as_posix(),
    ):
        entry = source_tree.get(relative_path)
        if entry is None:
            continue
        output_path = destination_path(relative_path)
        full_output_path = DESTINATION_REL / output_path
        if full_output_path in destination_paths:
            raise RuntimeError(
                f"multiple source files map to {full_output_path}"
            )
        destination_paths.add(full_output_path)

        oid = transform_blob(
            repo_root,
            entry,
            filename_replacements,
            macros,
        )
        index_entries.append(
            f"{entry.mode} {oid}\t{full_output_path.as_posix()}".encode() + b"\0"
        )

    for path, entry in sorted(
        untransformed_tree.items(),
        key=lambda item: item[0].as_posix(),
    ):
        if path in destination_paths:
            raise RuntimeError(
                "commit changes a Partition 2 path that is also generated "
                f"from Partition 1: {path.as_posix()}"
            )
        destination_paths.add(path)
        index_entries.append(
            f"{entry.mode} {entry.oid}\t{path.as_posix()}".encode() + b"\0"
        )

    if not index_entries:
        return run_git(repo_root, "mktree", input_data=b"").stdout.decode().strip()

    index_path = temporary_directory / f"{name}.index"
    environment = os.environ.copy()
    environment["GIT_INDEX_FILE"] = str(index_path)
    run_git(
        repo_root,
        "update-index",
        "--add",
        "-z",
        "--index-info",
        input_data=b"".join(index_entries),
        env=environment,
    )
    process = run_git(repo_root, "write-tree", env=environment)
    return process.stdout.decode().strip()


def changed_paths(repo_root: Path, base_tree: str, target_tree: str) -> list[str]:
    process = run_git(
        repo_root,
        "diff",
        "--name-only",
        "-z",
        base_tree,
        target_tree,
    )
    return [os.fsdecode(path) for path in process.stdout.split(b"\0") if path]


def ensure_paths_are_clean(repo_root: Path, paths: list[str]) -> None:
    unmerged = run_git(repo_root, "ls-files", "-u").stdout
    if unmerged:
        raise RuntimeError(
            "the repository already has unresolved conflicts; resolve them first"
        )

    process = run_git(
        repo_root,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--",
        *paths,
    )
    if process.stdout:
        changed = run_git(
            repo_root,
            "status",
            "--short",
            "--untracked-files=all",
            "--",
            *paths,
        ).stdout.decode().rstrip()
        raise RuntimeError(
            "target paths have local changes; commit or stash "
            f"them before applying:\n{changed}"
        )


def make_patch(repo_root: Path, base_tree: str, target_tree: str) -> bytes:
    return run_git(
        repo_root,
        "diff",
        "--binary",
        "--full-index",
        "--no-renames",
        base_tree,
        target_tree,
    ).stdout


def apply_patch(repo_root: Path, patch: bytes) -> int:
    process = run_git(
        repo_root,
        "apply",
        "--3way",
        "--whitespace=nowarn",
        input_data=patch,
        check=False,
        capture_output=False,
    )
    if process.returncode == 0:
        print("applied the transformed commit; changes are staged")
        return 0

    unmerged = run_git(
        repo_root,
        "diff",
        "--name-only",
        "--diff-filter=U",
    ).stdout.decode().splitlines()
    if unmerged:
        print(
            "the transformed commit has conflicts; conflict markers were left "
            "in:\n  " + "\n  ".join(unmerged),
            file=sys.stderr,
        )
        return process.returncode

    raise RuntimeError(
        "Git could not apply the transformed commit and did not leave "
        "resolvable conflicts"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("commit", help="commit (or revision) to port")
    parser.add_argument(
        "--parent",
        type=int,
        default=1,
        metavar="N",
        help="parent to diff against for a merge commit (default: 1)",
    )
    args = parser.parse_args()
    if args.parent < 1:
        parser.error("--parent must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    repo_root = find_repo_root()
    commit = resolve_commit(repo_root, args.commit)
    parent = select_parent(repo_root, commit, args.parent)

    commit_paths = changed_commit_paths(repo_root, parent, commit)
    source_prefix = SOURCE_REL.as_posix() + "/"
    changed_source_paths = {
        Path(path[len(source_prefix) :])
        for path in commit_paths
        if path.startswith(source_prefix)
    }
    if not changed_source_paths:
        raise RuntimeError(f"commit {commit[:12]} does not change Partition 1")
    untransformed_paths = [
        path for path in commit_paths if not path.startswith(source_prefix)
    ]

    generated_paths = {
        DESTINATION_REL / destination_path(path)
        for path in changed_source_paths
    }
    collisions = generated_paths & {
        Path(path) for path in untransformed_paths
    }
    if collisions:
        formatted_paths = "\n  ".join(
            path.as_posix() for path in sorted(collisions)
        )
        raise RuntimeError(
            "commit changes paths both directly and through the Partition 1 "
            f"transformation:\n  {formatted_paths}"
        )

    base_source_tree = read_source_tree(repo_root, parent)
    target_source_tree = read_source_tree(repo_root, commit)
    base_untransformed_tree = read_selected_tree_entries(
        repo_root,
        parent,
        untransformed_paths,
    )
    target_untransformed_tree = read_selected_tree_entries(
        repo_root,
        commit,
        untransformed_paths,
    )

    source_paths = set(base_source_tree) | set(target_source_tree)
    filename_replacements = replacement_filenames(sorted(source_paths))
    macros = find_implementation_macros(
        repo_root,
        (base_source_tree, target_source_tree),
    )

    with tempfile.TemporaryDirectory(prefix="partition1-to-partition2-") as temp:
        temporary_directory = Path(temp)
        base_tree = write_transformed_tree(
            repo_root,
            base_source_tree,
            changed_source_paths,
            base_untransformed_tree,
            filename_replacements,
            macros,
            temporary_directory,
            "base",
        )
        target_tree = write_transformed_tree(
            repo_root,
            target_source_tree,
            changed_source_paths,
            target_untransformed_tree,
            filename_replacements,
            macros,
            temporary_directory,
            "target",
        )

        paths = changed_paths(repo_root, base_tree, target_tree)
        ensure_paths_are_clean(repo_root, paths)
        patch = make_patch(repo_root, base_tree, target_tree)
        return apply_patch(repo_root, patch)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
