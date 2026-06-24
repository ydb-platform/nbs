from types import SimpleNamespace

import pytest

from . import helpers as h


def make_runner_release_payload(version="2.332.0", *, body=None, assets=None):
    if body is None:
        body = (
            f"- actions-runner-linux-x64-{version}.tar.gz "
            f"<!-- BEGIN SHA linux-x64 -->{'a' * 64}<!-- END SHA linux-x64 -->\n"
            f"- actions-runner-linux-arm64-{version}.tar.gz "
            f"<!-- BEGIN SHA linux-arm64 -->{'B' * 64}<!-- END SHA linux-arm64 -->"
        )
    if assets is None:
        assets = [
            {"name": f"actions-runner-linux-x64-{version}.tar.gz"},
            {"name": f"actions-runner-linux-arm64-{version}.tar.gz"},
        ]
    return {
        "tag_name": f"v{version}",
        "body": body,
        "assets": assets,
    }


def test_fetch_repo_variable_returns_repo_and_variable():
    variable = object()

    class FakeRepo:
        def get_variable(self, name):
            assert name == "RUNNER_VERSION"
            return variable

    class FakeGithub:
        def get_repo(self, repository):
            assert repository == "owner/repo"
            return FakeRepo()

    repo, result = h.fetch_repo_variable(FakeGithub(), "owner/repo", "RUNNER_VERSION")

    assert isinstance(repo, FakeRepo)
    assert result is variable


def test_format_github_response_debug_sanitizes_body_preview_newlines():
    response = SimpleNamespace(
        status_code=502,
        reason="Bad Gateway",
        headers={"content-type": "text/html"},
        text="<html>\nbad gateway</html>",
    )

    debug = h.format_github_response_debug(response)

    assert "status=502" in debug
    assert "reason='Bad Gateway'" in debug
    assert "content_type='text/html'" in debug
    assert "body_preview='<html>\\\\nbad gateway</html>'" in debug


def test_normalize_github_runner_version_strips_v_prefix():
    assert h.normalize_github_runner_version("v2.332.0") == "2.332.0"
    assert h.normalize_github_runner_version(" 2.332.0 ") == "2.332.0"


def test_normalize_github_runner_version_rejects_empty_value():
    with pytest.raises(ValueError, match="version is empty"):
        h.normalize_github_runner_version(" ")


def test_extract_github_runner_sha256_from_body_accepts_marker_whitespace():
    body = (
        "<!--  BEGIN SHA linux-x64  -->\n"
        f"{'A' * 64}\n"
        "<!--  END SHA linux-x64  -->"
    )

    assert h.extract_github_runner_sha256_from_body(body, "linux-x64") == "a" * 64


def test_extract_github_runner_sha256_from_body_rejects_missing_marker():
    with pytest.raises(ValueError, match="missing SHA-256 marker for linux-x64"):
        h.extract_github_runner_sha256_from_body("", "linux-x64")


def test_extract_github_runner_release_parses_version_assets_and_body_sha():
    release = h.extract_github_runner_release(make_runner_release_payload())

    assert release == h.GithubRunnerRelease(
        version="2.332.0",
        sha256_by_arch={"x64": "a" * 64, "arm64": "b" * 64},
    )


def test_extract_github_runner_release_rejects_missing_asset():
    payload = make_runner_release_payload(
        assets=[{"name": "actions-runner-linux-x64-2.332.0.tar.gz"}]
    )

    with pytest.raises(ValueError, match="missing asset.*linux-arm64"):
        h.extract_github_runner_release(payload)


def test_extract_github_runner_release_rejects_missing_tag_name():
    payload = make_runner_release_payload()
    del payload["tag_name"]

    with pytest.raises(ValueError, match="missing tag_name"):
        h.extract_github_runner_release(payload)


def test_get_github_runner_release_fetches_release_by_tag(monkeypatch):
    class FakeRepo:
        def get_release(self, tag):
            assert tag == "v2.331.0"
            payload = make_runner_release_payload("2.331.0")
            return SimpleNamespace(
                tag_name=payload["tag_name"],
                body=payload["body"],
                get_assets=lambda: [
                    SimpleNamespace(name=asset["name"]) for asset in payload["assets"]
                ],
            )

    class FakeGithub:
        def get_repo(self, repo):
            assert repo == "actions/runner"
            return FakeRepo()

    def fake_github_client(token):
        assert token == "token"
        return FakeGithub()

    monkeypatch.setattr(h, "github_client", fake_github_client)

    release = h.get_github_runner_release("v2.331.0", "token")

    assert release.version == "2.331.0"
    assert release.sha256_by_arch["x64"] == "a" * 64


def test_get_latest_github_runner_release_fetches_latest(monkeypatch):
    class FakeRepo:
        def get_latest_release(self):
            payload = make_runner_release_payload("2.332.0")
            return SimpleNamespace(
                tag_name=payload["tag_name"],
                body=payload["body"],
                get_assets=lambda: [
                    SimpleNamespace(name=asset["name"]) for asset in payload["assets"]
                ],
            )

    class FakeGithub:
        def get_repo(self, repo):
            assert repo == "actions/runner"
            return FakeRepo()

    def fake_github_client(token):
        assert token == "token"
        return FakeGithub()

    monkeypatch.setattr(h, "github_client", fake_github_client)

    release = h.get_latest_github_runner_release("token")

    assert release.version == "2.332.0"
    assert release.sha256_by_arch["arm64"] == "b" * 64


def test_get_jobs_raw_fetches_workflow_jobs_with_pygithub(monkeypatch):
    jobs = [SimpleNamespace(name="job-1")]

    class FakeRun:
        def jobs(self):
            return jobs

    class FakeRepo:
        def get_workflow_run(self, run_id):
            assert run_id == 123
            return FakeRun()

    class FakeGithub:
        def get_repo(self, repo):
            assert repo == "owner/repo"
            return FakeRepo()

    def fake_github_client(token):
        assert token == "token"
        return FakeGithub()

    monkeypatch.setattr(h, "github_client", fake_github_client)

    assert h.get_jobs_raw("token", "owner/repo", 123) == jobs


def test_resolve_github_runner_release_treats_empty_as_latest(monkeypatch):
    calls = []

    def fake_get_latest(github_token):
        calls.append(github_token)
        return h.GithubRunnerRelease(version="2.332.0", sha256_by_arch={})

    monkeypatch.setattr(h, "get_latest_github_runner_release", fake_get_latest)

    assert h.resolve_github_runner_release("", "token").version == "2.332.0"
    assert calls == ["token"]


def test_resolve_github_runner_release_fetches_pinned_version(monkeypatch):
    calls = []

    def fake_get_release(version, github_token):
        calls.append((version, github_token))
        return h.GithubRunnerRelease(version="2.331.0", sha256_by_arch={})

    monkeypatch.setattr(h, "get_github_runner_release", fake_get_release)

    assert h.resolve_github_runner_release("v2.331.0", "token").version == "2.331.0"
    assert calls == [("v2.331.0", "token")]
