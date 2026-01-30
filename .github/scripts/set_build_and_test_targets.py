#!/usr/bin/env python3
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List

from .helpers import (
    setup_logger,
    github_output,
    truthy,
    split_csv,
    json_obj,
    vm_suffix_for_component,
    is_san_preset,
    san_from_preset,
)
from .helpers import (
    COMPONENTS,
    SAN_COMPONENTS,
    SAN_SUFFIX,
    TEST_TYPE_REGULAR,
    TEST_TYPE_SAN,
)

# Explicit join rules (priority order):
# If ALL components in a group are present -> they will be emitted as ONE shard.
JOIN_GROUPS: List[List[str]] = [
    ["tasks", "storage"],
    # Add more rules if needed, e.g.:
    # ["disk_manager", "tasks"],
]


@dataclass(frozen=True)
class Inputs:
    build_target: str
    test_target: str
    build_preset: str

    split_runners: bool
    split_runners_san: Dict[str, bool]  # per-sanitizer split flags (optional)

    test_type: str
    number_of_retries: str

    @staticmethod
    def from_env(env=None) -> "Inputs":
        env = env or os.environ

        build_target = (env.get("BUILD_TARGET") or "").strip()
        test_target = (env.get("TEST_TARGET") or "").strip()
        build_preset = (env.get("BUILD_PRESET") or "").strip()

        split_runners = truthy(env.get("NEBIUS_SPLIT_RUNNERS"))
        split_runners_san = {
            "asan": truthy(env.get("NEBIUS_SPLIT_RUNNERS_ASAN")),
            "tsan": truthy(env.get("NEBIUS_SPLIT_RUNNERS_TSAN")),
            "msan": truthy(env.get("NEBIUS_SPLIT_RUNNERS_MSAN")),
            "ubsan": truthy(env.get("NEBIUS_SPLIT_RUNNERS_UBSAN")),
        }

        test_type = (env.get("TEST_TYPE") or "").strip()
        if not test_type:
            test_type = (
                TEST_TYPE_SAN if is_san_preset(build_preset) else TEST_TYPE_REGULAR
            )

        number_of_retries = (env.get("NUMBER_OF_RETRIES") or "").strip()
        if not number_of_retries:
            number_of_retries = "1" if is_san_preset(build_preset) else "3"

        return Inputs(
            build_target=build_target,
            test_target=test_target,
            build_preset=build_preset,
            split_runners=split_runners,
            split_runners_san=split_runners_san,
            test_type=test_type,
            number_of_retries=number_of_retries,
        )


def known_component_maps() -> tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns:
      build_root_by_component: {"blockstore": "cloud/blockstore/apps/", ...}
      test_root_by_component:  {"blockstore": "cloud/blockstore/", ...}
    """
    build_roots = {c: b for (c, b, _) in COMPONENTS}
    test_roots = {c: t for (c, _, t) in COMPONENTS}
    return build_roots, test_roots


def is_splittable_csv(csv_value: str, allowed_values: set[str]) -> bool:
    """
    Splittable iff:
      - non-empty
      - every element (after split_csv) is in allowed_values
    """
    parts = split_csv(csv_value)
    if not parts:
        return False
    return all(p in allowed_values for p in parts)


def _group_components(components: List[str]) -> List[List[str]]:
    """
    Apply JOIN_GROUPS rules over a component list.
    Returns list of groups (each group is list[str]) in stable order.
    """
    remaining = list(components)
    remaining_set = set(remaining)

    groups: List[List[str]] = []

    # Apply join rules in priority order
    for rule in JOIN_GROUPS:
        rule_set = set(rule)
        if rule_set.issubset(remaining_set):
            groups.append(rule[:])  # preserve rule order
            remaining_set -= rule_set
            remaining = [c for c in remaining if c not in rule_set]

    # Leftovers as singletons (preserve original order)
    for c in remaining:
        groups.append([c])

    return groups


def _suffix_for_group(group: List[str]) -> str:
    # Concatenate per-component suffixes; expected to yield things like "-tasks-storage"
    # If vm_suffix_for_component already includes "-", this produces a nice combined suffix.
    s = "".join(vm_suffix_for_component(c) for c in group)
    return s


def compute_matrix_include(inp: Inputs) -> str:
    build_roots, test_roots = known_component_maps()
    allowed_build = set(build_roots.values())
    allowed_test = set(test_roots.values())

    if not inp.build_target or not inp.test_target:
        raise ValueError("BUILD_TARGET and TEST_TARGET must be non-empty")

    preset = inp.build_preset
    is_san = is_san_preset(preset)
    san = san_from_preset(preset)

    # Decide whether splitting is allowed for this run.
    if is_san:
        split_enabled = bool(san) and inp.split_runners_san.get(san, False)
    else:
        split_enabled = inp.split_runners

    # Disable splitting if any custom targets are present
    build_splittable = is_splittable_csv(inp.build_target, allowed_build)
    test_splittable = is_splittable_csv(inp.test_target, allowed_test)
    split_enabled = split_enabled and build_splittable and test_splittable

    include: List[dict] = []

    if not split_enabled:
        vm_suffix = ""
        if is_san and san:
            vm_suffix = SAN_SUFFIX[san]
        include.append(
            {
                "build_target": inp.build_target,
                "test_target": inp.test_target,
                "vm_name_suffix": vm_suffix,
                "build_preset": preset,
                "test_type": inp.test_type,
                "number_of_retries": inp.number_of_retries,
                "san": san or "",
                "component": "all",
            }
        )
        return json_obj({"include": include})

    # Split by component (we already validated exact roots)
    build_items = split_csv(inp.build_target)
    test_items = split_csv(inp.test_target)

    comp_by_build = {v: k for k, v in build_roots.items()}
    comp_by_test = {v: k for k, v in test_roots.items()}

    # Build per-component mapping
    by_comp: Dict[str, tuple[str, str]] = {}
    for b in build_items:
        c = comp_by_build[b]
        by_comp[c] = (build_roots[c], test_roots[c])

    # Ensure any test-only entries are included (shouldn't happen normally, but safe)
    for t in test_items:
        c = comp_by_test[t]
        by_comp.setdefault(c, (build_roots.get(c, ""), test_roots.get(c, "")))

    comps = sorted(by_comp.keys())

    # For sanitizers, only split san-eligible components
    if is_san:
        comps = [c for c in comps if c in SAN_COMPONENTS]

    # If san split enabled but no san comps, fall back to singleton
    if is_san and not comps:
        vm_suffix = SAN_SUFFIX[san] if san else ""
        include.append(
            {
                "build_target": inp.build_target,
                "test_target": inp.test_target,
                "vm_name_suffix": vm_suffix,
                "build_preset": preset,
                "test_type": inp.test_type,
                "number_of_retries": inp.number_of_retries,
                "san": san or "",
                "component": "none",
            }
        )
        return json_obj({"include": include})

    # Apply explicit join rules (Option 1)
    groups = _group_components(comps)

    for group in groups:
        build_group = ",".join(build_roots[c] for c in group)
        test_group = ",".join(test_roots[c] for c in group)

        suffix = _suffix_for_group(group)
        if is_san and san:
            suffix = f"{SAN_SUFFIX[san]}{suffix}"

        include.append(
            {
                "build_target": build_group,
                "test_target": test_group,
                "vm_name_suffix": suffix,
                "build_preset": preset,
                "test_type": inp.test_type,
                "number_of_retries": inp.number_of_retries,
                "san": san or "",
                "component": "_".join(group),
            }
        )

    return json_obj({"include": include})


def main() -> int:
    logger = setup_logger()
    inp = Inputs.from_env()
    matrix_include = compute_matrix_include(inp)
    github_output(logger, "matrix_include", matrix_include)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
