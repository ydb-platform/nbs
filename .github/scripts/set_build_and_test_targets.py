#!/usr/bin/env python3
from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

from .helpers import setup_logger, github_output  # type: ignore


# (component, build_path, test_path)
COMPONENTS: List[Tuple[str, str, str]] = [
    ("blockstore", "cloud/blockstore/apps/", "cloud/blockstore/"),
    ("filestore", "cloud/filestore/apps/", "cloud/filestore/"),
    ("disk_manager", "cloud/disk_manager/", "cloud/disk_manager/"),
    ("tasks", "cloud/tasks/", "cloud/tasks/"),
    ("storage", "cloud/storage/", "cloud/storage/"),
]

SAN_COMPONENTS = {"blockstore", "filestore", "storage"}
SAN_TYPES = ("asan", "tsan", "msan", "ubsan")


def truthy(v: str | None) -> bool:
    return (v or "").strip().lower() == "true"


def csv_join(parts: List[str]) -> str:
    return ",".join(parts)


def split_csv(csv_value: str) -> List[str]:
    return [p.strip() for p in csv_value.split(",") if p.strip()]


def json_array(items: List[str]) -> str:
    return json.dumps(items, separators=(",", ":"))


def json_obj(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))


def vm_suffix_for_component(component: str) -> str:
    return f"-{component}"


@dataclass(frozen=True)
class Inputs:
    contains: Dict[str, bool]
    has_san: Dict[str, bool]

    split_runners: bool
    split_runners_san: Dict[str, bool]  # per-sanitizer split flags

    @staticmethod
    def from_env(env: Dict[str, str] | None = None) -> "Inputs":
        env = env or os.environ

        contains = {
            "blockstore": truthy(env.get("CONTAINS_BLOCKSTORE")),
            "filestore": truthy(env.get("CONTAINS_FILESTORE")),
            "disk_manager": truthy(env.get("CONTAINS_DISK_MANAGER")),
            "tasks": truthy(env.get("CONTAINS_TASKS")),
            "storage": truthy(env.get("CONTAINS_STORAGE")),
        }
        has_san = {
            "asan": truthy(env.get("HAS_ASAN_LABEL")),
            "tsan": truthy(env.get("HAS_TSAN_LABEL")),
            "msan": truthy(env.get("HAS_MSAN_LABEL")),
            "ubsan": truthy(env.get("HAS_UBSAN_LABEL")),
        }

        split_runners = truthy(env.get("NEBIUS_SPLIT_RUNNERS"))
        split_runners_san = {
            "asan": truthy(env.get("NEBIUS_SPLIT_RUNNERS_ASAN")),
            "tsan": truthy(env.get("NEBIUS_SPLIT_RUNNERS_TSAN")),
            "msan": truthy(env.get("NEBIUS_SPLIT_RUNNERS_MSAN")),
            "ubsan": truthy(env.get("NEBIUS_SPLIT_RUNNERS_UBSAN")),
        }

        return Inputs(
            contains=contains,
            has_san=has_san,
            split_runners=split_runners,
            split_runners_san=split_runners_san,
        )


@dataclass(frozen=True)
class Outputs:
    # CSVs (existing interface)
    build_target: str
    test_target: str
    build_target_san: Dict[str, str]
    test_target_san: Dict[str, str]

    # JSON arrays (kept for compatibility)
    build_matrix: str
    test_matrix: str
    build_matrix_san: Dict[str, str]
    test_matrix_san: Dict[str, str]

    # Paired include matrices with vm_name_suffix
    matrix_include: str
    matrix_include_san: Dict[str, str]


def compute_outputs(inp: Inputs) -> Outputs:
    true_count = sum(1 for c, _, _ in COMPONENTS if inp.contains.get(c, False))
    false_count = len(COMPONENTS) - true_count

    build_parts: List[str] = []
    test_parts: List[str] = []

    build_san_parts: Dict[str, List[str]] = {k: [] for k in SAN_TYPES}
    test_san_parts: Dict[str, List[str]] = {k: [] for k in SAN_TYPES}

    selected_components: List[Tuple[str, str, str]] = []

    def add(component: str, build_path: str, test_path: str) -> None:
        build_parts.append(build_path)
        test_parts.append(test_path)
        selected_components.append((component, build_path, test_path))

        if component in SAN_COMPONENTS:
            for san in SAN_TYPES:
                if inp.has_san.get(san, False):
                    build_san_parts[san].append(build_path)
                    test_san_parts[san].append(test_path)

    # Keep exact bash semantics: if all true OR all false -> everything
    if true_count == len(COMPONENTS) or false_count == len(COMPONENTS):
        for c, b, t in COMPONENTS:
            add(c, b, t)
    else:
        for c, b, t in COMPONENTS:
            if inp.contains.get(c, False):
                add(c, b, t)

    build_target = csv_join(build_parts)
    test_target = csv_join(test_parts)

    build_target_san = {san: csv_join(build_san_parts[san]) for san in SAN_TYPES}
    test_target_san = {san: csv_join(test_san_parts[san]) for san in SAN_TYPES}

    # Regular array matrices
    if inp.split_runners:
        build_matrix = json_array(split_csv(build_target))
        test_matrix = json_array(split_csv(test_target))
    else:
        build_matrix = json_array([build_target])
        test_matrix = json_array([test_target])

    # Sanitizer array matrices: split only if NEBIUS_SPLIT_RUNNERS_<SAN> is true
    build_matrix_san: Dict[str, str] = {}
    test_matrix_san: Dict[str, str] = {}
    for san in SAN_TYPES:
        if inp.split_runners_san.get(san, False):
            build_matrix_san[san] = json_array(split_csv(build_target_san[san]))
            test_matrix_san[san] = json_array(split_csv(test_target_san[san]))
        else:
            build_matrix_san[san] = json_array([build_target_san[san]])
            test_matrix_san[san] = json_array([test_target_san[san]])

    # Regular include matrix (paired + suffix)
    if inp.split_runners:
        include = [
            {
                "build_target": b,
                "test_target": t,
                "vm_name_suffix": vm_suffix_for_component(c),
            }
            for (c, b, t) in selected_components
        ]
    else:
        include = [
            {
                "build_target": build_target,
                "test_target": test_target,
                "vm_name_suffix": "",
            }
        ]
    matrix_include = json_obj({"include": include})

    # Sanitizer include matrices:
    # - default singleton (no split)
    # - if NEBIUS_SPLIT_RUNNERS_<SAN> == true -> per-component include entries
    matrix_include_san: Dict[str, str] = {}
    for san in SAN_TYPES:
        if inp.split_runners_san.get(san, False):
            # only components that were selected AND are san-eligible are present in san lists
            include_san = [
                {
                    "build_target": b,
                    "test_target": t,
                    "vm_name_suffix": vm_suffix_for_component(c),
                }
                for (c, b, t) in selected_components
                if c in SAN_COMPONENTS and inp.has_san.get(san, False)
            ]

            # If sanitizer label is on but nothing eligible was selected, keep a single empty entry
            if not include_san:
                include_san = [
                    {
                        "build_target": build_target_san[san],
                        "test_target": test_target_san[san],
                        "vm_name_suffix": "",
                    }
                ]
        else:
            include_san = [
                {
                    "build_target": build_target_san[san],
                    "test_target": test_target_san[san],
                    "vm_name_suffix": "",
                }
            ]

        matrix_include_san[san] = json_obj({"include": include_san})

    return Outputs(
        build_target=build_target,
        test_target=test_target,
        build_target_san=build_target_san,
        test_target_san=test_target_san,
        build_matrix=build_matrix,
        test_matrix=test_matrix,
        build_matrix_san=build_matrix_san,
        test_matrix_san=test_matrix_san,
        matrix_include=matrix_include,
        matrix_include_san=matrix_include_san,
    )


def main() -> int:
    logger = setup_logger()
    inp = Inputs.from_env()
    out = compute_outputs(inp)

    # Existing outputs:
    github_output(logger, "build_target", out.build_target)
    github_output(logger, "test_target", out.test_target)

    github_output(logger, "build_target_asan", out.build_target_san["asan"])
    github_output(logger, "build_target_tsan", out.build_target_san["tsan"])
    github_output(logger, "build_target_msan", out.build_target_san["msan"])
    github_output(logger, "build_target_ubsan", out.build_target_san["ubsan"])

    github_output(logger, "test_target_asan", out.test_target_san["asan"])
    github_output(logger, "test_target_tsan", out.test_target_san["tsan"])
    github_output(logger, "test_target_msan", out.test_target_san["msan"])
    github_output(logger, "test_target_ubsan", out.test_target_san["ubsan"])

    github_output(logger, "build_matrix", out.build_matrix)
    github_output(logger, "test_matrix", out.test_matrix)

    github_output(logger, "build_matrix_asan", out.build_matrix_san["asan"])
    github_output(logger, "build_matrix_tsan", out.build_matrix_san["tsan"])
    github_output(logger, "build_matrix_msan", out.build_matrix_san["msan"])
    github_output(logger, "build_matrix_ubsan", out.build_matrix_san["ubsan"])

    github_output(logger, "test_matrix_asan", out.test_matrix_san["asan"])
    github_output(logger, "test_matrix_tsan", out.test_matrix_san["tsan"])
    github_output(logger, "test_matrix_msan", out.test_matrix_san["msan"])
    github_output(logger, "test_matrix_ubsan", out.test_matrix_san["ubsan"])

    # Include matrices:
    github_output(logger, "matrix_include", out.matrix_include)
    github_output(logger, "matrix_include_asan", out.matrix_include_san["asan"])
    github_output(logger, "matrix_include_tsan", out.matrix_include_san["tsan"])
    github_output(logger, "matrix_include_msan", out.matrix_include_san["msan"])
    github_output(logger, "matrix_include_ubsan", out.matrix_include_san["ubsan"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
