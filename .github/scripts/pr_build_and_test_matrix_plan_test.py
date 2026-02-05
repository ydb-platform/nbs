from .pr_build_and_test_matrix_plan import (
    Inputs,
    build_matrix,
    compute_targets,
    decide_modes,
)
from .helpers import (
    COMPONENTS,
    SAN_TYPES,
    TEST_TYPE_REGULAR,
    TEST_TYPE_SAN,
    DEFAULT_BUILD_TARGET,
    DEFAULT_TEST_TARGET,
)


def mk_inputs(
    *,
    scheduling_type: str = "",
    on_demand_label: bool = False,
    pooled_label: bool = False,
    hybrid_label: bool = False,
    contains=None,
    san=None,
    large_tests: bool = False,
) -> Inputs:
    """Helper to build Inputs with sensible defaults."""
    # default: all components false
    full_contains = {name: False for name, _, _ in COMPONENTS}
    if contains:
        full_contains.update(contains)

    # default: all sanitizers false
    full_san = {k: False for k in SAN_TYPES}
    if san:
        full_san.update(san)

    return Inputs(
        scheduling_type=scheduling_type,
        has_on_demand_label=on_demand_label,
        has_pooled_label=pooled_label,
        has_hybrid_label=hybrid_label,
        contains=full_contains,
        has_san=full_san,
        has_large_tests_label=large_tests,
    )


def test_compute_targets_all_false_means_all_components():
    inp = mk_inputs()
    build_target, test_target, build_san, test_san = compute_targets(inp)

    assert build_target == DEFAULT_BUILD_TARGET
    assert test_target == DEFAULT_TEST_TARGET

    # no san labels -> san targets should be empty csv
    for k in SAN_TYPES:
        assert build_san[k] == ""
        assert test_san[k] == ""


def test_compute_targets_all_true_means_all_components():
    inp = mk_inputs(
        contains={
            "blockstore": True,
            "filestore": True,
            "disk_manager": True,
            "tasks": True,
            "storage": True,
        }
    )
    build_target, test_target, _, _ = compute_targets(inp)

    assert build_target == DEFAULT_BUILD_TARGET
    assert test_target == DEFAULT_TEST_TARGET


def test_compute_targets_mixed_selects_only_true_components():
    inp = mk_inputs(contains={"tasks": True, "storage": True})
    build_target, test_target, _, _ = compute_targets(inp)

    assert build_target == "cloud/tasks/,cloud/storage/"
    assert test_target == "cloud/tasks/,cloud/storage/"


def test_compute_targets_sanitizer_targets_only_include_san_components():
    inp = mk_inputs(
        contains={"disk_manager": True, "tasks": True, "storage": True},
        san={"asan": True},
    )
    build_target, _, build_san, test_san = compute_targets(inp)

    # disk_manager + tasks + storage for regular
    assert build_target == "cloud/disk_manager/,cloud/tasks/,cloud/storage/"
    # only storage is in SAN_COMPONENTS, so san targets get only storage
    assert build_san["asan"] == "cloud/storage/"
    assert test_san["asan"] == "cloud/storage/"

    # other sanitizers still empty
    for san in ("tsan", "msan", "ubsan"):
        assert build_san[san] == ""
        assert test_san[san] == ""


def test_decide_modes_default_regular_goes_on_demand():
    # scheduling_type empty, no pooled/hybrid labels -> on_demand
    inp = mk_inputs()
    modes = decide_modes(inp)
    assert modes == ["on_demand"]


def test_decide_modes_pooled_label_disables_on_demand():
    # scheduling_type regular + pooled label -> pooled only
    inp = mk_inputs(scheduling_type="regular", pooled_label=True)
    modes = decide_modes(inp)
    assert modes == ["pooled"]


def test_decide_modes_explicit_pooled_type():
    inp = mk_inputs(scheduling_type="pooled")
    modes = decide_modes(inp)
    assert modes == ["pooled"]


def test_decide_modes_explicit_hybrid_type():
    inp = mk_inputs(scheduling_type="hybrid")
    modes = decide_modes(inp)
    assert modes == ["hybrid"]


def test_build_matrix_single_mode_no_sanitizers():
    inp = mk_inputs(
        contains={
            "blockstore": True,
            "filestore": True,
        }
    )
    matrix = build_matrix(inp)

    # one mode (on_demand) and no san -> single row
    assert len(matrix) == 1
    row = matrix[0]

    assert row["mode"] == "on_demand"
    assert row["san"] == ""
    assert row["build_preset"] == "relwithdebinfo"
    assert row["test_type"] == TEST_TYPE_REGULAR
    assert row["test_size"] == "small,medium"
    assert row["vm_name_suffix"] == ""
    assert row["number_of_retries"] == 3

    # targets match compute_targets()
    bt, tt, _, _ = compute_targets(inp)
    assert row["build_target"] == bt
    assert row["test_target"] == tt


def test_build_matrix_multiple_sanitizers():
    # blockstore + filestore are san-eligible, and asan+tsan labels are set
    inp = mk_inputs(
        contains={"blockstore": True, "filestore": True},
        san={"asan": True, "tsan": True},
    )
    matrix = build_matrix(inp)

    # one regular + 2 sanitizer rows (asan, tsan)
    assert len(matrix) == 3

    modes = {row["san"]: row for row in matrix}

    # regular row
    regular = modes[""]
    assert regular["build_preset"] == "relwithdebinfo"
    assert regular["test_type"] == TEST_TYPE_REGULAR
    assert regular["vm_name_suffix"] == ""
    assert regular["number_of_retries"] == 3

    # asan row
    asan = modes["asan"]
    assert asan["build_preset"] == "release-asan"
    assert asan["test_type"] == TEST_TYPE_SAN
    assert asan["vm_name_suffix"] == "-asan"
    assert asan["number_of_retries"] == 1

    # tsan row
    tsan = modes["tsan"]
    assert tsan["build_preset"] == "release-tsan"
    assert tsan["test_type"] == TEST_TYPE_SAN
    assert tsan["vm_name_suffix"] == "-tsan"
    assert tsan["number_of_retries"] == 1

    # rows are all mode=on_demand in this setup
    assert {row["mode"] for row in matrix} == {"on_demand"}


def test_build_matrix_large_tests_propagates_to_all_rows():
    inp = mk_inputs(
        contains={"storage": True},
        san={"asan": True},
        large_tests=True,
    )
    matrix = build_matrix(inp)

    # one regular + one asan (storage is SAN-eligible)
    assert len(matrix) == 2
    for row in matrix:
        assert row["test_size"] == "small,medium,large"


def test_build_matrix_skips_empty_san_targets():
    # tasks is not SAN-eligible; SAN targets become empty and should be ignored
    inp = mk_inputs(
        contains={"tasks": True},
        san={"asan": True},
    )
    matrix = build_matrix(inp)

    # only the regular row should remain
    assert len(matrix) == 1
    assert matrix[0]["san"] == ""
