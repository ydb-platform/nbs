from __future__ import annotations

from types import SimpleNamespace

import pytest

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


@pytest.mark.parametrize("cleanup_conclusion", ["success", "failure", "timed_out"])
def test_resolve_workflow_conclusion_includes_cleanup_in_the_linked_run(
    cleanup_conclusion,
):
    marker = fwc.workload_job_marker("relwithdebinfo", "blockstore", "native")
    jobs = [
        SimpleNamespace(
            id=123,
            name=f"Build and test / Build and test {marker}",
            conclusion="success",
        ),
        SimpleNamespace(
            id=124,
            name=f"Build and test / Release self-hosted runner {marker}",
            conclusion=cleanup_conclusion,
        ),
    ]

    def jobs_for_run(run_id):
        assert run_id == 456
        return jobs

    assert (
        fwc.resolve_job_conclusion(
            "https://github.com/org/repo/actions/runs/456/job/123",
            "relwithdebinfo",
            "blockstore",
            "native",
            999,
            jobs_for_run,
            include_related_jobs=True,
        )
        == cleanup_conclusion
    )
    # Cleanup jobs must not make the build-job fallback ambiguous.
    assert (
        fwc.find_workload_job_conclusion(jobs, "relwithdebinfo", "blockstore", "native")
        == "success"
    )


def test_workflow_conclusion_ignores_failures_in_other_matrix_entries():
    marker = fwc.workload_job_marker("relwithdebinfo", "blockstore", "native")
    jobs = [SimpleNamespace(name=f"Build and test {marker}", conclusion="success")]
    for preset, component, platform in [
        ("relwithdebinfo", "filestore", "native"),
        ("release-asan", "blockstore", "native"),
        ("relwithdebinfo", "blockstore", "default-linux-armv9a_grace"),
    ]:
        other = fwc.workload_job_marker(preset, component, platform)
        jobs.append(
            SimpleNamespace(
                name=f"Build and test {marker} / Release self-hosted runner {other}",
                conclusion="failure",
            )
        )
    assert (
        fwc.find_workflow_conclusion(jobs, "relwithdebinfo", "blockstore", "native")
        == "success"
    )


@pytest.mark.parametrize(
    "conclusions,expected",
    [
        (["success", "skipped"], "success"),
        (["skipped", "skipped"], "skipped"),
        (["success", "cancelled"], "cancelled"),
        (["success", None], None),
        (["success", "unknown"], None),
        ([], None),
    ],
)
def test_workflow_conclusion_handles_optional_or_unfinished_jobs(conclusions, expected):
    marker = fwc.workload_job_marker("relwithdebinfo", "blockstore", "native")
    jobs = [
        SimpleNamespace(name=f"Job {marker}", conclusion=conclusion)
        for conclusion in conclusions
    ]
    assert (
        fwc.find_workflow_conclusion(jobs, "relwithdebinfo", "blockstore", "native")
        == expected
    )


@pytest.mark.parametrize(
    "execution,cleanup,other,cleanup_only",
    [
        ("success", "failure", None, True),
        ("failure", "success", None, False),
        ("failure", "failure", None, False),
        ("timed_out", "failure", None, False),
        ("success", "failure", "failure", False),
        ("success", "failure", "skipped", True),
        ("success", None, None, False),
        ("success", "success", None, False),
    ],
)
def test_workflow_outcome_separates_execution_and_cleanup(
    execution, cleanup, other, cleanup_only
):
    marker = fwc.workload_job_marker("relwithdebinfo", "filestore", "native")
    jobs = [
        SimpleNamespace(id=1, name=f"Build and test {marker}", conclusion=execution),
        SimpleNamespace(
            id=2, name=f"Release self-hosted runner {marker}", conclusion=cleanup
        ),
    ]
    if other is not None:
        jobs.append(SimpleNamespace(id=3, name=f"Debug {marker}", conclusion=other))

    def jobs_for_run(run_id):
        assert run_id == 123
        return jobs

    result = fwc.resolve_workflow_outcome(
        "https://github.com/org/repo/actions/runs/123/job/1",
        "relwithdebinfo",
        "filestore",
        "native",
        999,
        jobs_for_run,
    )
    assert result.execution == execution
    assert result.workflow == fwc.find_workflow_conclusion(
        jobs, "relwithdebinfo", "filestore", "native"
    )
    assert result.cleanup_only_failure is cleanup_only
