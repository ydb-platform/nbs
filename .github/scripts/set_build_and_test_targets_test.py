import json

from .set_build_and_test_targets import Inputs, compute_outputs, COMPONENTS


def mk_inputs(*, contains=None, san=None, split=False, split_san=None):
    contains = contains or {}
    san = san or {}
    split_san = split_san or {}

    full_contains = {name: False for name, _, _ in COMPONENTS}
    full_contains.update(contains)

    full_san = {"asan": False, "tsan": False, "msan": False, "ubsan": False}
    full_san.update(san)

    full_split_san = {"asan": False, "tsan": False, "msan": False, "ubsan": False}
    full_split_san.update(split_san)

    return Inputs(
        contains=full_contains,
        has_san=full_san,
        split_runners=split,
        split_runners_san=full_split_san,
    )


def test_all_false_means_all_components():
    out = compute_outputs(mk_inputs())
    assert out.build_target == "cloud/blockstore/apps/,cloud/filestore/apps/,cloud/disk_manager/,cloud/tasks/,cloud/storage/"
    assert out.test_target == "cloud/blockstore/,cloud/filestore/,cloud/disk_manager/,cloud/tasks/,cloud/storage/"


def test_all_true_means_all_components():
    out = compute_outputs(
        mk_inputs(
            contains={
                "blockstore": True,
                "filestore": True,
                "disk_manager": True,
                "tasks": True,
                "storage": True,
            }
        )
    )
    assert out.build_target == "cloud/blockstore/apps/,cloud/filestore/apps/,cloud/disk_manager/,cloud/tasks/,cloud/storage/"
    assert out.test_target == "cloud/blockstore/,cloud/filestore/,cloud/disk_manager/,cloud/tasks/,cloud/storage/"


def test_mixed_selects_only_true_components():
    out = compute_outputs(mk_inputs(contains={"tasks": True, "storage": True}))
    assert out.build_target == "cloud/tasks/,cloud/storage/"
    assert out.test_target == "cloud/tasks/,cloud/storage/"


def test_sanitizer_targets_only_include_san_components():
    out = compute_outputs(mk_inputs(contains={"disk_manager": True, "tasks": True, "storage": True}, san={"asan": True}))
    assert out.build_target == "cloud/disk_manager/,cloud/tasks/,cloud/storage/"
    assert out.build_target_san["asan"] == "cloud/storage/"
    assert out.test_target_san["asan"] == "cloud/storage/"


def test_split_true_splits_regular_matrix_arrays():
    out = compute_outputs(mk_inputs(contains={"blockstore": True, "tasks": True}, split=True))
    assert json.loads(out.build_matrix) == ["cloud/blockstore/apps/", "cloud/tasks/"]
    assert json.loads(out.test_matrix) == ["cloud/blockstore/", "cloud/tasks/"]


def test_split_false_singleton_regular_matrix_arrays():
    out = compute_outputs(mk_inputs(contains={"blockstore": True, "tasks": True}, split=False))
    assert json.loads(out.build_matrix) == ["cloud/blockstore/apps/,cloud/tasks/"]
    assert json.loads(out.test_matrix) == ["cloud/blockstore/,cloud/tasks/"]


def test_san_matrix_arrays_default_singleton_even_if_regular_split_true():
    out = compute_outputs(mk_inputs(contains={"blockstore": True, "filestore": True}, san={"asan": True}, split=True))
    assert json.loads(out.build_matrix_san["asan"]) == ["cloud/blockstore/apps/,cloud/filestore/apps/"]
    assert json.loads(out.test_matrix_san["asan"]) == ["cloud/blockstore/,cloud/filestore/"]


def test_matrix_include_split_true_has_per_component_entries_and_suffix():
    out = compute_outputs(mk_inputs(contains={"blockstore": True, "tasks": True}, split=True))
    m = json.loads(out.matrix_include)
    assert m["include"] == [
        {"build_target": "cloud/blockstore/apps/", "test_target": "cloud/blockstore/", "vm_name_suffix": "-blockstore"},
        {"build_target": "cloud/tasks/", "test_target": "cloud/tasks/", "vm_name_suffix": "-tasks"},
    ]


def test_matrix_include_split_false_is_singleton_like_san():
    out = compute_outputs(mk_inputs(contains={"blockstore": True, "tasks": True}, split=False))
    m = json.loads(out.matrix_include)
    assert m["include"] == [
        {
            "build_target": "cloud/blockstore/apps/,cloud/tasks/",
            "test_target": "cloud/blockstore/,cloud/tasks/",
            "vm_name_suffix": "",
        }
    ]


def test_matrix_include_san_default_singleton():
    out = compute_outputs(mk_inputs(contains={"blockstore": True, "filestore": True}, san={"asan": True}, split=True))
    m = json.loads(out.matrix_include_san["asan"])
    assert m["include"] == [
        {
            "build_target": "cloud/blockstore/apps/,cloud/filestore/apps/",
            "test_target": "cloud/blockstore/,cloud/filestore/",
            "vm_name_suffix": "",
        }
    ]


def test_matrix_include_san_can_split_when_flag_enabled():
    out = compute_outputs(
        mk_inputs(
            contains={"blockstore": True, "filestore": True, "tasks": True},
            san={"asan": True},
            split=False,  # regular split doesn't matter
            split_san={"asan": True},
        )
    )
    m = json.loads(out.matrix_include_san["asan"])
    # tasks is not san-eligible => should NOT be present
    assert m["include"] == [
        {"build_target": "cloud/blockstore/apps/", "test_target": "cloud/blockstore/", "vm_name_suffix": "-blockstore"},
        {"build_target": "cloud/filestore/apps/", "test_target": "cloud/filestore/", "vm_name_suffix": "-filestore"},
    ]


def test_san_matrix_arrays_can_split_when_flag_enabled():
    out = compute_outputs(
        mk_inputs(
            contains={"blockstore": True, "filestore": True, "tasks": True},
            san={"asan": True},
            split_san={"asan": True},
        )
    )
    assert json.loads(out.build_matrix_san["asan"]) == ["cloud/blockstore/apps/", "cloud/filestore/apps/"]
    assert json.loads(out.test_matrix_san["asan"]) == ["cloud/blockstore/", "cloud/filestore/"]
