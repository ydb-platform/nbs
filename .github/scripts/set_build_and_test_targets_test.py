import json
import pytest

from .set_build_and_test_targets import Inputs, compute_matrix_include


def mk(
    *,
    build_target: str,
    test_target: str,
    build_preset: str,
    split: bool = False,
    split_san=None,
    test_type: str = "",
    retries: str = "",
):
    split_san = split_san or {
        "asan": False,
        "tsan": False,
        "msan": False,
        "ubsan": False,
    }
    return Inputs(
        build_target=build_target,
        test_target=test_target,
        build_preset=build_preset,
        split_runners=split,
        split_runners_san=split_san,
        test_type=test_type,
        number_of_retries=retries,
    )


def parse(inp: Inputs):
    obj = json.loads(compute_matrix_include(inp))
    return obj["include"]


def test_regular_no_split_singleton():
    inc = parse(
        mk(
            build_target="cloud/blockstore/apps/,cloud/tasks/",
            test_target="cloud/blockstore/,cloud/tasks/",
            build_preset="relwithdebinfo",
            split=False,
        )
    )
    assert len(inc) == 1
    assert inc[0]["component"] == "all"
    assert inc[0]["vm_name_suffix"] == ""
    assert inc[0]["build_preset"] == "relwithdebinfo"


def test_regular_split_only_when_targets_are_exact_roots():
    inc = parse(
        mk(
            build_target="cloud/blockstore/apps/,cloud/tasks/",
            test_target="cloud/blockstore/,cloud/tasks/",
            build_preset="relwithdebinfo",
            split=True,
        )
    )
    assert len(inc) == 2
    assert {r["component"] for r in inc} == {"blockstore", "tasks"}
    by = {r["component"]: r for r in inc}
    assert by["blockstore"]["build_target"] == "cloud/blockstore/apps/"
    assert by["blockstore"]["test_target"] == "cloud/blockstore/"
    assert by["tasks"]["build_target"] == "cloud/tasks/"
    assert by["tasks"]["test_target"] == "cloud/tasks/"


def test_join_rule_tasks_storage_applies_when_both_present():
    inc = parse(
        mk(
            build_target="cloud/tasks/,cloud/storage/",
            test_target="cloud/tasks/,cloud/storage/",
            build_preset="relwithdebinfo",
            split=True,
        )
    )
    assert len(inc) == 1
    assert inc[0]["component"] == "tasks_storage"
    assert inc[0]["build_target"] == "cloud/tasks/,cloud/storage/"
    assert inc[0]["test_target"] == "cloud/tasks/,cloud/storage/"
    # suffix should include both (exact string depends on vm_suffix_for_component)
    assert "tasks" in inc[0]["vm_name_suffix"]
    assert "storage" in inc[0]["vm_name_suffix"]


def test_join_rule_tasks_storage_with_third_component():
    inc = parse(
        mk(
            build_target="cloud/blockstore/apps/,cloud/tasks/,cloud/storage/",
            test_target="cloud/blockstore/,cloud/tasks/,cloud/storage/",
            build_preset="relwithdebinfo",
            split=True,
        )
    )
    assert len(inc) == 2
    assert {r["component"] for r in inc} == {"blockstore", "tasks_storage"}
    by = {r["component"]: r for r in inc}
    assert by["blockstore"]["build_target"] == "cloud/blockstore/apps/"
    assert by["tasks_storage"]["build_target"] == "cloud/tasks/,cloud/storage/"


def test_join_rule_tasks_and_other_component():
    inc = parse(
        mk(
            build_target="cloud/blockstore/apps/,cloud/tasks/",
            test_target="cloud/blockstore/,cloud/tasks/",
            build_preset="relwithdebinfo",
            split=True,
        )
    )
    assert len(inc) == 2
    assert {r["component"] for r in inc} == {"blockstore", "tasks"}
    by = {r["component"]: r for r in inc}
    assert by["blockstore"]["build_target"] == "cloud/blockstore/apps/"
    assert by["tasks"]["build_target"] == "cloud/tasks/"


def test_custom_target_disables_split():
    inc = parse(
        mk(
            build_target="cloud/blockstore/libs/,cloud/tasks/",
            test_target="cloud/blockstore/,cloud/tasks/",
            build_preset="relwithdebinfo",
            split=True,
        )
    )
    assert len(inc) == 1
    assert inc[0]["build_target"] == "cloud/blockstore/libs/,cloud/tasks/"


def test_san_preset_uses_per_san_split_flag_off_by_default():
    inc = parse(
        mk(
            build_target="cloud/blockstore/apps/,cloud/filestore/apps/",
            test_target="cloud/blockstore/,cloud/filestore/",
            build_preset="release-asan",
            split=True,  # ignored for san
            split_san={"asan": False, "tsan": False, "msan": False, "ubsan": False},
        )
    )
    assert len(inc) == 1
    assert inc[0]["vm_name_suffix"].startswith("-asan")


def test_san_split_enabled_splits_only_san_components():
    # tasks is not san-eligible, so asan split should only include blockstore
    inc = parse(
        mk(
            build_target="cloud/blockstore/apps/,cloud/tasks/",
            test_target="cloud/blockstore/,cloud/tasks/",
            build_preset="release-asan",
            split_san={"asan": True, "tsan": False, "msan": False, "ubsan": False},
        )
    )
    assert len(inc) == 1
    assert inc[0]["component"] == "blockstore"
    assert inc[0]["build_target"] == "cloud/blockstore/apps/"
    assert inc[0]["vm_name_suffix"].startswith("-asan")


def test_san_custom_target_disables_split_even_if_flag_true():
    inc = parse(
        mk(
            build_target="cloud/blockstore/libs/",
            test_target="cloud/blockstore/",
            build_preset="release-asan",
            split_san={"asan": True, "tsan": False, "msan": False, "ubsan": False},
        )
    )
    assert len(inc) == 1
    assert inc[0]["build_target"] == "cloud/blockstore/libs/"


def test_empty_targets_raise():
    with pytest.raises(ValueError):
        parse(
            mk(
                build_target="",
                test_target="",
                build_preset="relwithdebinfo",
            )
        )
