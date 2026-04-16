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
