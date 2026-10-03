from __future__ import annotations

from scripts import helpers as h
from types import SimpleNamespace

from scripts.tests import workload_comment as wc


def test_iter_components_preserves_matrix_order() -> None:
    matrix_include = """
    {
      "include": [
        {"component": "blockstore"},
        {"component": "tasks_storage"},
        {"component": "filestore"}
      ]
    }
    """

    assert wc.iter_components(matrix_include) == [
        "blockstore",
        "tasks_storage",
        "filestore",
    ]


def test_find_current_job_url_falls_back_to_run_url(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")

    def fake_get_jobs(github: object, repo: str, run_id: int) -> list[object]:
        assert github is not None
        assert repo == "org/repo"
        assert run_id == 123
        return []

    monkeypatch.setattr(h, "get_jobs", fake_get_jobs)

    assert h.find_current_job_url(object(), "job", "runner") == (
        "https://github.com/org/repo/actions/runs/123"
    )


def test_find_current_job_url_matches_reusable_workflow_job_name(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")

    def fake_get_jobs(github: object, repo: str, run_id: int) -> list[object]:
        assert github is not None
        assert repo == "org/repo"
        assert run_id == 123
        return [
            SimpleNamespace(
                name=(
                    "On-demand build and test / Build and test "
                    "[build_preset=relwithdebinfo component=blockstore] "
                    "[id=1 ip=10.0.0.1]"
                ),
                runner_name="runner-1",
                status="in_progress",
                html_url="https://github.com/org/repo/actions/runs/123/job/999",
            )
        ]

    monkeypatch.setattr(h, "get_jobs", fake_get_jobs)

    assert (
        h.find_current_job_url(
            object(),
            "Build and test [build_preset=relwithdebinfo component=blockstore] [id=1 ip=10.0.0.1]",
            "runner-1",
        )
        == "https://github.com/org/repo/actions/runs/123/job/999"
    )


def test_find_current_job_url_prefers_runner_specific_match(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")

    def fake_get_jobs(github: object, repo: str, run_id: int) -> list[object]:
        assert github is not None
        assert repo == "org/repo"
        assert run_id == 123
        return [
            SimpleNamespace(
                name="Pooled build and test / Build and test [build_preset=relwithdebinfo component=blockstore]",
                runner_name="runner-a",
                status="in_progress",
                html_url="https://github.com/org/repo/actions/runs/123/job/111",
            ),
            SimpleNamespace(
                name="Pooled build and test / Build and test [build_preset=relwithdebinfo component=blockstore]",
                runner_name="runner-b",
                status="in_progress",
                html_url="https://github.com/org/repo/actions/runs/123/job/222",
            ),
        ]

    monkeypatch.setattr(h, "get_jobs", fake_get_jobs)

    assert (
        h.find_current_job_url(
            object(),
            "Build and test [build_preset=relwithdebinfo component=blockstore]",
            "runner-b",
        )
        == "https://github.com/org/repo/actions/runs/123/job/222"
    )
