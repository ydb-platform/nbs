from __future__ import annotations

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
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setattr(wc, "fetch_jobs", lambda: [])

    assert wc.find_current_job_url("job", "runner") == (
        "https://github.com/org/repo/actions/runs/123"
    )


def test_find_current_job_url_matches_reusable_workflow_job_name(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setattr(
        wc,
        "fetch_jobs",
        lambda: [
            {
                "name": "On-demand build and test / Build and test relwithdebinfo [id=1 ip=10.0.0.1]",
                "runner_name": "runner-1",
                "status": "in_progress",
                "html_url": "https://github.com/org/repo/actions/runs/123/job/999",
            }
        ],
    )

    assert (
        wc.find_current_job_url(
            "Build and test relwithdebinfo [id=1 ip=10.0.0.1]",
            "runner-1",
        )
        == "https://github.com/org/repo/actions/runs/123/job/999"
    )


def test_find_current_job_url_prefers_runner_specific_match(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setattr(
        wc,
        "fetch_jobs",
        lambda: [
            {
                "name": "Pooled build and test / Build and test relwithdebinfo",
                "runner_name": "runner-a",
                "status": "in_progress",
                "html_url": "https://github.com/org/repo/actions/runs/123/job/111",
            },
            {
                "name": "Pooled build and test / Build and test relwithdebinfo",
                "runner_name": "runner-b",
                "status": "in_progress",
                "html_url": "https://github.com/org/repo/actions/runs/123/job/222",
            },
        ],
    )

    assert (
        wc.find_current_job_url("Build and test relwithdebinfo", "runner-b")
        == "https://github.com/org/repo/actions/runs/123/job/222"
    )
