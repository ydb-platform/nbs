#!/usr/bin/env python3
from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Dict, List

from .helpers import setup_logger, github_output, truthy, csv_join
from .helpers import (
    COMPONENTS,
    SAN_COMPONENTS,
    SAN_TYPES,
    SAN_PRESET,
    TEST_TYPE_REGULAR,
    TEST_TYPE_SAN,
)


@dataclass(frozen=True)
class Inputs:
    scheduling_type: str
    has_on_demand_label: bool
    has_pooled_label: bool
    has_hybrid_label: bool

    contains: Dict[str, bool]
    has_san: Dict[str, bool]
    has_large_tests_label: bool

    @staticmethod
    def from_env(env: Dict[str, str] | None = None) -> "Inputs":
        env = env or os.environ
        return Inputs(
            scheduling_type=(env.get("SCHEDULING_TYPE") or "").strip(),
            has_on_demand_label=truthy(env.get("HAS_ON_DEMAND_LABEL")),
            has_pooled_label=truthy(env.get("HAS_POOLED_LABEL")),
            has_hybrid_label=truthy(env.get("HAS_HYBRID_LABEL")),
            contains={
                "blockstore": truthy(env.get("CONTAINS_BLOCKSTORE")),
                "filestore": truthy(env.get("CONTAINS_FILESTORE")),
                "disk_manager": truthy(env.get("CONTAINS_DISK_MANAGER")),
                "tasks": truthy(env.get("CONTAINS_TASKS")),
                "storage": truthy(env.get("CONTAINS_STORAGE")),
            },
            has_san={
                "asan": truthy(env.get("HAS_ASAN_LABEL")),
                "tsan": truthy(env.get("HAS_TSAN_LABEL")),
                "msan": truthy(env.get("HAS_MSAN_LABEL")),
                "ubsan": truthy(env.get("HAS_UBSAN_LABEL")),
            },
            has_large_tests_label=truthy(env.get("HAS_LARGE_TESTS_LABEL")),
        )


def decide_modes(inp: Inputs) -> List[str]:
    st = inp.scheduling_type

    run_pooled = (st == "pooled") or inp.has_pooled_label
    run_hybrid = (st == "hybrid") or inp.has_hybrid_label

    regular_or_empty = (st == "regular") or (st == "")
    non_regular_non_empty = (st != "regular") and (st != "")

    run_on_demand = (
        regular_or_empty and (not inp.has_pooled_label) and (not inp.has_hybrid_label)
    ) or (non_regular_non_empty and inp.has_on_demand_label)

    modes: List[str] = []
    if run_on_demand:
        modes.append("on_demand")
    if run_pooled:
        modes.append("pooled")
    if run_hybrid:
        modes.append("hybrid")
    return modes


def compute_targets(inp: Inputs) -> tuple[str, str, Dict[str, str], Dict[str, str]]:
    true_count = sum(1 for c, _, _ in COMPONENTS if inp.contains.get(c, False))
    false_count = len(COMPONENTS) - true_count

    build_parts: List[str] = []
    test_parts: List[str] = []
    build_san_parts: Dict[str, List[str]] = {k: [] for k in SAN_TYPES}
    test_san_parts: Dict[str, List[str]] = {k: [] for k in SAN_TYPES}

    def add(component: str, b: str, t: str) -> None:
        build_parts.append(b)
        test_parts.append(t)
        if component in SAN_COMPONENTS:
            for san in SAN_TYPES:
                if inp.has_san.get(san, False):
                    build_san_parts[san].append(b)
                    test_san_parts[san].append(t)

    # Preserve old semantics: all true OR all false => all components
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
    return build_target, test_target, build_target_san, test_target_san


def build_matrix(inp: Inputs) -> list[dict]:
    """
    Pure function: given Inputs, produce the list of matrix rows (dicts) that main() will wrap into {"include": [...]}.

    This is what we unit-test.
    """
    modes = decide_modes(inp)
    build_target, test_target, build_target_san, test_target_san = compute_targets(inp)
    test_size = "small,medium,large" if inp.has_large_tests_label else "small,medium"

    include: List[dict] = []

    for mode in modes:
        # regular row
        include.append(
            {
                "mode": mode,
                "san": "",
                "build_preset": "relwithdebinfo",
                "test_type": TEST_TYPE_REGULAR,
                "test_size": test_size,
                "build_target": build_target,
                "test_target": test_target,
                "vm_name_suffix": "",
                "number_of_retries": 3,
            }
        )

        # sanitizer rows (multiple can be present)
        for san in SAN_TYPES:
            if inp.has_san.get(san, False):
                preset, suffix = SAN_PRESET[san]
                # Skip sanitizer rows that would carry empty targets; they do nothing and
                # can break downstream steps.
                if not build_target_san[san] or not test_target_san[san]:
                    continue
                include.append(
                    {
                        "mode": mode,
                        "san": san,
                        "build_preset": preset,
                        "test_type": TEST_TYPE_SAN,
                        "test_size": test_size,
                        "build_target": build_target_san[san],
                        "test_target": test_target_san[san],
                        "vm_name_suffix": suffix,
                        "number_of_retries": 1,
                    }
                )

    return include


def main() -> int:
    logger = setup_logger()
    inp = Inputs.from_env()

    include = build_matrix(inp)
    matrix_include = json.dumps({"include": include}, separators=(",", ":"))
    github_output(logger, "matrix_include", matrix_include)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
