from __future__ import annotations

from types import SimpleNamespace

from scripts.tests import finalize_workload_comments as fwc


def test_iter_full_build_presets_uses_target_platform() -> None:
    matrix_include = """
    {
      "include": [
        {"build_preset": "relwithdebinfo"},
        {"build_preset": "release-asan"},
        {
          "build_preset": "relwithdebinfo",
          "target_platform": "default-linux-armv9a_grace"
        }
      ]
    }
    """

    assert fwc.iter_full_build_presets(matrix_include, "linux-x86_64") == [
        "linux-arm64-relwithdebinfo",
        "linux-x86_64-release-asan",
        "linux-x86_64-relwithdebinfo",
    ]
    assert fwc.iter_build_preset_contexts(matrix_include, "linux-x86_64") == [
        (
            "linux-arm64-relwithdebinfo",
            "relwithdebinfo",
            "default-linux-armv9a_grace",
        ),
        ("linux-x86_64-release-asan", "release-asan", "native"),
        ("linux-x86_64-relwithdebinfo", "relwithdebinfo", "native"),
    ]


def test_iter_full_build_presets_returns_empty_list_for_empty_matrix_output() -> None:
    assert fwc.iter_full_build_presets("", "linux-x86_64") == []
    assert fwc.iter_full_build_presets("  \n", "linux-x86_64") == []


def test_resolve_job_conclusion_falls_back_to_workload_job_name() -> None:
    jobs = [
        SimpleNamespace(
            id=123,
            name=(
                "On-demand build and test / Build and test "
                "[build_preset=relwithdebinfo component=blockstore "
                "target_platform=native]"
            ),
            conclusion="failure",
        )
    ]
    requested_run_ids: list[int] = []

    def jobs_for_run(run_id: int) -> list[object]:
        requested_run_ids.append(run_id)
        return jobs

    assert (
        fwc.resolve_job_conclusion(
            "",
            "relwithdebinfo",
            "blockstore",
            "native",
            456,
            jobs_for_run,
        )
        == "failure"
    )
    assert requested_run_ids == [456]


def test_resolve_job_conclusion_prefers_job_url() -> None:
    jobs = [
        SimpleNamespace(
            id=123,
            name=(
                "Build and test [build_preset=other component=other "
                "target_platform=native]"
            ),
            conclusion="cancelled",
        )
    ]

    def jobs_for_run(run_id: int) -> list[object]:
        assert run_id == 456
        return jobs

    assert (
        fwc.resolve_job_conclusion(
            "https://github.com/org/repo/actions/runs/456/job/123",
            "relwithdebinfo",
            "blockstore",
            "native",
            456,
            jobs_for_run,
        )
        == "cancelled"
    )


def test_find_workload_job_conclusion_rejects_ambiguous_matches() -> None:
    name = (
        "Build and test [build_preset=relwithdebinfo component=blockstore "
        "target_platform=native]"
    )
    jobs = [
        SimpleNamespace(name=name, conclusion="failure"),
        SimpleNamespace(name=name, conclusion="skipped"),
    ]

    assert (
        fwc.find_workload_job_conclusion(
            jobs,
            "relwithdebinfo",
            "blockstore",
            "native",
        )
        is None
    )


def test_find_workload_job_conclusion_matches_skipped_reusable_job() -> None:
    jobs = [
        SimpleNamespace(
            name=(
                "On-demand build and test / Build and test NBS "
                "[build_preset=relwithdebinfo component=blockstore "
                "target_platform=native]"
            ),
            conclusion="skipped",
        )
    ]

    assert (
        fwc.find_workload_job_conclusion(
            jobs,
            "relwithdebinfo",
            "blockstore",
            "native",
        )
        == "skipped"
    )


def test_find_workload_job_conclusion_distinguishes_target_platforms() -> None:
    jobs = [
        SimpleNamespace(
            name=(
                "Build and test [build_preset=relwithdebinfo component=blockstore "
                "target_platform=native]"
            ),
            conclusion="failure",
        ),
        SimpleNamespace(
            name=(
                "Build and test [build_preset=relwithdebinfo component=blockstore "
                "target_platform=default-linux-armv9a_grace]"
            ),
            conclusion="cancelled",
        ),
    ]

    assert (
        fwc.find_workload_job_conclusion(
            jobs,
            "relwithdebinfo",
            "blockstore",
            "default-linux-armv9a_grace",
        )
        == "cancelled"
    )
