#!/usr/bin/env python3

"""Replace the Partition 2 sources with a namespaced copy of Partition 1.

The script copies Git-tracked files from storage/partition to
storage/partition2 and applies the mechanical changes needed for both
implementations to coexist in one build:

  * part.* and part_*.{h,cpp} become part2.* and part2_*.{h,cpp};
  * self-includes and ya.make dependencies point at partition2;
  * NPartition becomes NPartition2;
  * the public partition API include points at api/partition2.h;
  * implementation-owned BLOCKSTORE_PARTITION_* macros get a PARTITION2
    prefix;
  * tests instantiate a BlockStorePartition2 tablet.

By default, an existing locally modified partition2 tree is not overwritten.
Pass --force when discarding those changes is intentional.
"""

from __future__ import annotations

import argparse
import filecmp
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


SOURCE_REL = Path("cloud/blockstore/libs/storage/partition")
DESTINATION_REL = Path("cloud/blockstore/libs/storage/partition2")

SOURCE_DEPENDENCY = "cloud/blockstore/libs/storage/partition"
DESTINATION_DEPENDENCY = "cloud/blockstore/libs/storage/partition2"

PARTITION_API = "cloud/blockstore/libs/storage/api/partition.h"
PARTITION2_API = "cloud/blockstore/libs/storage/api/partition2.h"

BINARY_SUFFIXES = {".blob"}


def run_git(repo_root: Path, *args: str) -> str:
    process = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode:
        message = process.stderr.strip() or process.stdout.strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {message}")
    return process.stdout


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


def tracked_source_files(repo_root: Path) -> list[Path]:
    output = subprocess.run(
        [
            "git",
            "-C",
            str(repo_root),
            "ls-files",
            "-z",
            "--",
            SOURCE_REL.as_posix(),
        ],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout

    prefix = SOURCE_REL.as_posix() + "/"
    result = []
    for item in output.split(b"\0"):
        if not item:
            continue
        path = item.decode()
        if not path.startswith(prefix):
            raise RuntimeError(f"unexpected source path reported by Git: {path}")
        result.append(Path(path[len(prefix) :]))

    if not result:
        raise RuntimeError(f"no tracked files found under {SOURCE_REL}")
    return sorted(result)


def renamed_component(component: str) -> str:
    """Translate only Partition 1 implementation filenames.

    A blanket part -> part2 replacement would corrupt dependencies such as
    partition_common/part_thread_safe_state.h and testlib/part_client.h.
    """

    return re.sub(r"^part(?=\.|_)", "part2", component)


def destination_path(relative_path: Path) -> Path:
    return Path(*(renamed_component(part) for part in relative_path.parts))


def implementation_macros(source: Path, source_files: list[Path]) -> set[str]:
    """Find macros owned by Partition 1 rather than its public dependencies."""

    result: set[str] = set()
    pattern = re.compile(r"^\s*#\s*define\s+(BLOCKSTORE_PARTITION[A-Z0-9_]*)", re.M)
    for relative_path in source_files:
        if relative_path.suffix in BINARY_SUFFIXES:
            continue
        text = (source / relative_path).read_text()
        result.update(pattern.findall(text))
    return result


def replacement_filenames(source_files: list[Path]) -> dict[str, str]:
    result: dict[str, str] = {}
    for relative_path in source_files:
        old_name = relative_path.name
        new_name = renamed_component(old_name)
        if old_name != new_name:
            previous = result.setdefault(old_name, new_name)
            if previous != new_name:
                raise RuntimeError(f"conflicting rename for {old_name}")
    return result


def replace_token(text: str, old: str, new: str) -> str:
    pattern = rf"(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])"
    return re.sub(pattern, lambda _: new, text)


def transform_text(
    text: str,
    filename_replacements: dict[str, str],
    macros: set[str],
) -> str:
    text = text.replace(PARTITION_API, PARTITION2_API)
    text = re.sub(
        re.escape(SOURCE_DEPENDENCY) + r"(?![A-Za-z0-9_])",
        DESTINATION_DEPENDENCY,
        text,
    )

    for old_name in sorted(filename_replacements, key=len, reverse=True):
        text = replace_token(text, old_name, filename_replacements[old_name])

    text = replace_token(text, "NPartition", "NPartition2")

    for macro in sorted(macros, key=len, reverse=True):
        partition2_macro = macro.replace(
            "BLOCKSTORE_PARTITION",
            "BLOCKSTORE_PARTITION2",
            1,
        )
        text = replace_token(text, macro, partition2_macro)

    text = re.sub(
        r"TTabletTypes::BlockStorePartition(?!2)",
        "TTabletTypes::BlockStorePartition2",
        text,
    )
    text = replace_token(
        text,
        "TPartition1BlockIndexTest",
        "TPartition2BlockIndexTest",
    )
    text = re.sub(
        r"(?<=Y_UNIT_TEST_SUITE\()TPartition(?!2)",
        "TPartition2",
        text,
    )
    return text


def add_compatible_factory_overload(staging: Path) -> None:
    """Retain Partition 2's factory ABI while copying Partition 1 internals."""

    header = staging / "part2.h"
    source = staging / "part2.cpp"

    header_marker = (
        "}   // namespace NCloud::NBlockStore::NStorage::NPartition2\n"
    )
    header_overload = """\
// Compatibility entry point for existing Partition 2 callers.  Partition 2
// historically did not receive the volume partition index.
NActors::IActorPtr CreatePartitionTablet(
    const NActors::TActorId& owner,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 siblingCount,
    const NActors::TActorId& volumeActorId,
    ui64 volumeTabletId);

"""
    header_text = header.read_text()
    if header_text.count(header_marker) != 1:
        raise RuntimeError("could not locate the namespace end in generated part2.h")
    header.write_text(
        header_text.replace(header_marker, header_overload + header_marker)
    )

    source_marker = (
        "}   // namespace NCloud::NBlockStore::NStorage::NPartition2\n"
    )
    source_overload = """\
IActorPtr CreatePartitionTablet(
    const TActorId& owner,
    TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 siblingCount,
    const NActors::TActorId& volumeActorId,
    ui64 volumeTabletId)
{
    return CreatePartitionTablet(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(blockDigestGenerator),
        std::move(partitionConfig),
        storageAccessMode,
        0,
        siblingCount,
        volumeActorId,
        volumeTabletId);
}

"""
    source_text = source.read_text()
    if source_text.count(source_marker) != 1:
        raise RuntimeError("could not locate the namespace end in generated part2.cpp")
    source.write_text(
        source_text.replace(source_marker, source_overload + source_marker)
    )


def build_staging_tree(
    source: Path,
    staging: Path,
    source_files: list[Path],
) -> None:
    filename_replacements = replacement_filenames(source_files)
    macros = implementation_macros(source, source_files)
    generated_paths: set[Path] = set()

    for relative_path in source_files:
        output_relative_path = destination_path(relative_path)
        if output_relative_path in generated_paths:
            raise RuntimeError(f"multiple inputs generate {output_relative_path}")
        generated_paths.add(output_relative_path)

        input_path = source / relative_path
        output_path = staging / output_relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if relative_path.suffix not in BINARY_SUFFIXES:
            text = transform_text(
                input_path.read_text(),
                filename_replacements,
                macros,
            )
            output_path.write_text(text)
            shutil.copymode(input_path, output_path)
        else:
            shutil.copy2(input_path, output_path)

    add_compatible_factory_overload(staging)
    validate_generated_tree(staging, source_files)


def validate_generated_tree(staging: Path, source_files: list[Path]) -> None:
    required = {
        Path("part2.h"),
        Path("part2.cpp"),
        Path("part2_actor.h"),
        Path("part2_actor.cpp"),
        Path("model/ya.make"),
        Path("ut/ya.make"),
        Path("ya.make"),
    }
    missing = sorted(path for path in required if not (staging / path).is_file())
    if missing:
        raise RuntimeError(
            "generated tree is missing required files: "
            + ", ".join(str(path) for path in missing)
        )

    expected_count = len(source_files)
    actual_count = sum(path.is_file() for path in staging.rglob("*"))
    if actual_count != expected_count:
        raise RuntimeError(
            f"expected {expected_count} generated files, found {actual_count}"
        )

    stale_dependency = re.compile(
        re.escape(SOURCE_DEPENDENCY) + r"(?![A-Za-z0-9_])"
    )
    stale_namespace = re.compile(r"\bNPartition\b")
    for path in staging.rglob("*"):
        if not path.is_file() or path.suffix in BINARY_SUFFIXES:
            continue
        text = path.read_text()
        if stale_dependency.search(text):
            raise RuntimeError(f"stale Partition 1 dependency in {path}")
        if stale_namespace.search(text):
            raise RuntimeError(f"stale NPartition namespace in {path}")
        if PARTITION_API in text:
            raise RuntimeError(f"stale Partition 1 API include in {path}")


def trees_equal(left: Path, right: Path) -> bool:
    comparison = filecmp.dircmp(left, right)
    if comparison.left_only or comparison.right_only or comparison.funny_files:
        return False
    if any(
        not filecmp.cmp(left / name, right / name, shallow=False)
        for name in comparison.common_files
    ):
        return False
    return all(
        trees_equal(left / name, right / name)
        for name in comparison.common_dirs
    )


def ensure_safe_output(repo_root: Path, output: Path) -> None:
    source = repo_root / SOURCE_REL
    forbidden = {
        Path("/"),
        repo_root,
        source,
    }
    if (
        output in forbidden
        or output in source.parents
        or source in output.parents
    ):
        raise RuntimeError(f"refusing to replace unsafe output path: {output}")


def replace_tree(staging: Path, destination: Path, temporary_root: Path) -> None:
    old_destination = temporary_root / "old-partition2"
    if destination.exists():
        destination.rename(old_destination)

    try:
        staging.rename(destination)
    except BaseException:
        if old_destination.exists() and not destination.exists():
            old_destination.rename(destination)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="build and validate the transformed tree without replacing partition2",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite local changes or an existing custom --output directory",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write to another directory (primarily useful for validation)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = find_repo_root()
    source = repo_root / SOURCE_REL
    destination = (
        args.output.resolve()
        if args.output
        else repo_root / DESTINATION_REL
    )

    if not source.is_dir():
        raise RuntimeError(f"source directory does not exist: {source}")
    ensure_safe_output(repo_root, destination)

    source_files = tracked_source_files(repo_root)
    destination.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix=".partition2-replacement-",
        dir=destination.parent,
    ) as temporary_directory:
        temporary_root = Path(temporary_directory)
        staging = temporary_root / "generated-partition2"
        staging.mkdir()
        build_staging_tree(source, staging, source_files)

        if destination.is_dir() and trees_equal(staging, destination):
            print(
                f"{destination} already contains the generated "
                f"{len(source_files)}-file tree"
            )
            return 0

        if args.dry_run:
            print(
                f"validated {len(source_files)} files; "
                f"would replace {destination}"
            )
            return 0

        if destination.exists() and not args.force:
            if args.output:
                raise RuntimeError(
                    f"output already exists: {destination}; pass --force to replace it"
                )

            status = run_git(
                repo_root,
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--",
                DESTINATION_REL.as_posix(),
            ).strip()
            if status:
                raise RuntimeError(
                    "partition2 has local changes; commit/stash them or pass "
                    "--force to discard them"
                )

        replace_tree(staging, destination, temporary_root)

    print(
        f"replaced {destination} with {len(source_files)} transformed "
        "Partition 1 files"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
