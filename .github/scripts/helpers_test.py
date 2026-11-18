from types import SimpleNamespace
import json

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


def test_fetch_repo_variable_retries_github_failures(monkeypatch):
    variable = object()
    calls = []
    sleeps = []

    class FakeRepo:
        def get_variable(self, name):
            assert name == "RUNNER_VERSION"
            return variable

    class FakeGithub:
        def get_repo(self, repository):
            assert repository == "owner/repo"
            calls.append(repository)
            if len(calls) == 1:
                raise h.GithubException(502, {"message": "bad gateway"})
            return FakeRepo()

    monkeypatch.setattr(h.time, "sleep", sleeps.append)

    repo, result = h.fetch_repo_variable(FakeGithub(), "owner/repo", "RUNNER_VERSION")

    assert isinstance(repo, FakeRepo)
    assert result is variable
    assert calls == ["owner/repo", "owner/repo"]
    assert sleeps == [h.GITHUB_API_RETRY_INTERVAL_SEC]


def test_fetch_github_team_public_keys_fetches_member_keys():
    class FakeKey:
        def __init__(self, key):
            self.key = key

    class FakeMember:
        def __init__(self, login, keys):
            self.login = login
            self.keys = keys

        def get_keys(self):
            return [FakeKey(key) for key in self.keys]

    class FakeTeam:
        def get_members(self):
            return [
                FakeMember("alice", ["ssh-rsa alice-1", "ssh-ed25519 alice-2"]),
                FakeMember("bob", ["ssh-rsa bob-1"]),
            ]

    class FakeOrg:
        def get_team_by_slug(self, slug):
            assert slug == "team"
            return FakeTeam()

    class FakeGithub:
        def get_organization(self, org):
            assert org == "org"
            return FakeOrg()

    assert h.fetch_github_team_public_keys(FakeGithub(), "org", "team") == [
        "ssh-rsa alice-1",
        "ssh-ed25519 alice-2",
        "ssh-rsa bob-1",
    ]


def test_fetch_github_team_public_keys_retries_member_fetch(monkeypatch):
    sleeps = []

    class FakeKey:
        key = "ssh-rsa alice-1"

    class FakeMember:
        login = "alice"

        def get_keys(self):
            return [FakeKey()]

    class FakeTeam:
        def __init__(self):
            self.calls = 0

        def get_members(self):
            self.calls += 1
            if self.calls == 1:
                raise h.GithubException(502, {"message": "bad gateway"})
            return [FakeMember()]

    class FakeOrg:
        def __init__(self, team):
            self.team = team

        def get_team_by_slug(self, slug):
            assert slug == "team"
            return self.team

    class FakeGithub:
        def __init__(self, team):
            self.team = team

        def get_organization(self, org):
            assert org == "org"
            return FakeOrg(self.team)

    team = FakeTeam()
    monkeypatch.setattr(h.time, "sleep", sleeps.append)

    assert h.fetch_github_team_public_keys(FakeGithub(team), "org", "team") == [
        "ssh-rsa alice-1"
    ]
    assert team.calls == 2
    assert sleeps == [h.GITHUB_API_RETRY_INTERVAL_SEC]


def test_fetch_github_team_public_keys_retries_member_fetch_connection_error(
    monkeypatch,
):
    sleeps = []

    class FakeKey:
        key = "ssh-rsa alice-1"

    class FakeMember:
        login = "alice"

        def get_keys(self):
            return [FakeKey()]

    class FakeTeam:
        def __init__(self):
            self.calls = 0

        def get_members(self):
            self.calls += 1
            if self.calls == 1:
                raise h.requests.exceptions.ConnectionError("dns failure")
            return [FakeMember()]

    class FakeOrg:
        def __init__(self, team):
            self.team = team

        def get_team_by_slug(self, slug):
            assert slug == "team"
            return self.team

    class FakeGithub:
        def __init__(self, team):
            self.team = team

        def get_organization(self, org):
            assert org == "org"
            return FakeOrg(self.team)

    team = FakeTeam()
    monkeypatch.setattr(h.time, "sleep", sleeps.append)

    assert h.fetch_github_team_public_keys(FakeGithub(team), "org", "team") == [
        "ssh-rsa alice-1"
    ]
    assert team.calls == 2
    assert sleeps == [h.GITHUB_API_RETRY_INTERVAL_SEC]


def test_fetch_github_team_public_keys_retries_member_key_fetch(monkeypatch):
    sleeps = []

    class FakeKey:
        key = "ssh-rsa alice-1"

    class FakeMember:
        login = "alice"

        def __init__(self):
            self.calls = 0

        def get_keys(self):
            self.calls += 1
            if self.calls == 1:
                raise h.GithubException(502, {"message": "bad gateway"})
            return [FakeKey()]

    class FakeTeam:
        def __init__(self, member):
            self.member = member

        def get_members(self):
            return [self.member]

    class FakeOrg:
        def __init__(self, team):
            self.team = team

        def get_team_by_slug(self, slug):
            assert slug == "team"
            return self.team

    class FakeGithub:
        def __init__(self, team):
            self.team = team

        def get_organization(self, org):
            assert org == "org"
            return FakeOrg(self.team)

    member = FakeMember()
    monkeypatch.setattr(h.time, "sleep", sleeps.append)

    assert h.fetch_github_team_public_keys(
        FakeGithub(FakeTeam(member)), "org", "team"
    ) == ["ssh-rsa alice-1"]
    assert member.calls == 2
    assert sleeps == [h.GITHUB_API_RETRY_INTERVAL_SEC]


def test_fetch_github_team_public_keys_retries_member_key_fetch_timeout(
    monkeypatch,
):
    sleeps = []

    class FakeKey:
        key = "ssh-rsa alice-1"

    class FakeMember:
        login = "alice"

        def __init__(self):
            self.calls = 0

        def get_keys(self):
            self.calls += 1
            if self.calls == 1:
                raise h.requests.exceptions.Timeout("read timed out")
            return [FakeKey()]

    class FakeTeam:
        def __init__(self, member):
            self.member = member

        def get_members(self):
            return [self.member]

    class FakeOrg:
        def __init__(self, team):
            self.team = team

        def get_team_by_slug(self, slug):
            assert slug == "team"
            return self.team

    class FakeGithub:
        def __init__(self, team):
            self.team = team

        def get_organization(self, org):
            assert org == "org"
            return FakeOrg(self.team)

    member = FakeMember()
    monkeypatch.setattr(h.time, "sleep", sleeps.append)

    assert h.fetch_github_team_public_keys(
        FakeGithub(FakeTeam(member)), "org", "team"
    ) == ["ssh-rsa alice-1"]
    assert member.calls == 2
    assert sleeps == [h.GITHUB_API_RETRY_INTERVAL_SEC]


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


def test_github_client_from_env_uses_github_token(monkeypatch):
    calls = []
    github = object()

    def fake_github_client(token):
        calls.append(token)
        return github

    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setattr(h, "github_client", fake_github_client)

    assert h.github_client_from_env() is github
    assert calls == ["token"]


def test_github_client_from_env_requires_github_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    with pytest.raises(Exception, match="GITHUB_TOKEN environment variable is not set"):
        h.github_client_from_env()


def test_get_github_runner_release_fetches_release_by_tag():
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

    release = h.get_github_runner_release(FakeGithub(), "v2.331.0")

    assert release.version == "2.331.0"
    assert release.sha256_by_arch["x64"] == "a" * 64


def test_get_latest_github_runner_release_fetches_latest():
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

    release = h.get_latest_github_runner_release(FakeGithub())

    assert release.version == "2.332.0"
    assert release.sha256_by_arch["arm64"] == "b" * 64


def test_get_jobs_fetches_latest_or_one_attempt():
    jobs = [
        SimpleNamespace(name="old", run_attempt=1),
        SimpleNamespace(name="current", run_attempt=2),
    ]
    filters = []

    class FakeRun:
        def jobs(self, *, _filter):
            filters.append(_filter)
            return jobs

    class FakeRepo:
        def get_workflow_run(self, run_id):
            assert run_id == 123
            return FakeRun()

    class FakeGithub:
        def get_repo(self, repo):
            assert repo == "owner/repo"
            return FakeRepo()

    github = FakeGithub()
    assert h.get_jobs(github, "owner/repo", 123) == jobs
    assert [
        job.name for job in h.get_jobs(github, "owner/repo", 123, run_attempt=2)
    ] == ["current"]
    assert filters == ["latest", "all"]


def test_get_jobs_retries_pygithub_failures(monkeypatch):
    jobs = [SimpleNamespace(name="job-1")]
    attempts = []
    sleeps = []

    class FakeRun:
        def jobs(self, *, _filter):
            assert _filter == "latest"
            return jobs

    class FakeRepo:
        def get_workflow_run(self, run_id):
            assert run_id == 123
            attempts.append(run_id)
            if len(attempts) == 1:
                raise h.requests.exceptions.Timeout("temporary")
            return FakeRun()

    class FakeGithub:
        def get_repo(self, repo):
            assert repo == "owner/repo"
            return FakeRepo()

    monkeypatch.setattr(h.time, "sleep", sleeps.append)

    assert h.get_jobs(FakeGithub(), "owner/repo", 123) == jobs
    assert attempts == [123, 123]
    assert sleeps == [h.GITHUB_API_RETRY_INTERVAL_SEC]


def test_parse_actions_job_url_extracts_run_and_job_ids() -> None:
    assert h.parse_actions_job_url(
        "https://github.com/org/repo/actions/runs/28388298305/job/84109112950"
    ) == (28388298305, 84109112950)
    assert h.parse_actions_job_url("https://github.com/org/repo/actions/runs/1") is None


def test_load_github_event(monkeypatch, tmp_path):
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps({"pull_request": {"number": 42}}))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_path))

    assert h.load_github_event() == {"pull_request": {"number": 42}}


def test_pull_request_from_event_uses_github_event_payload():
    event = {"pull_request": {"number": 42}}

    class FakeGithub:
        def create_from_raw_data(self, cls, data):
            assert cls.__name__ == "PullRequest"
            return SimpleNamespace(number=data["number"])

    pr = h.pull_request_from_event(FakeGithub(), event)

    assert pr.number == 42


def test_resolve_github_runner_release_treats_empty_as_latest(monkeypatch):
    calls = []
    github = object()

    def fake_get_latest(received_github):
        calls.append(received_github)
        return h.GithubRunnerRelease(version="2.332.0", sha256_by_arch={})

    monkeypatch.setattr(h, "get_latest_github_runner_release", fake_get_latest)

    assert h.resolve_github_runner_release(github, "").version == "2.332.0"
    assert calls == [github]


def test_resolve_github_runner_release_fetches_pinned_version(monkeypatch):
    calls = []
    github = object()

    def fake_get_release(received_github, version):
        calls.append((received_github, version))
        return h.GithubRunnerRelease(version="2.331.0", sha256_by_arch={})

    monkeypatch.setattr(h, "get_github_runner_release", fake_get_release)

    assert h.resolve_github_runner_release(github, "v2.331.0").version == "2.331.0"
    assert calls == [(github, "v2.331.0")]
