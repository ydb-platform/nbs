from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from github.GithubException import GithubException

from scripts.policy.api import GitHubApi, _read_workflow_window
from scripts.policy.model import API_VERSION
from scripts.policy.types import EventRule, PolicyApiError, PolicySummary

WORKFLOW = ".github/workflows/example.yaml"
SINCE = datetime(2026, 9, 27, tzinfo=timezone.utc)
UNTIL = datetime(2026, 9, 28, tzinfo=timezone.utc)


def test_from_env_uses_only_github_token(monkeypatch) -> None:
    client_factory = Mock()
    monkeypatch.setattr("scripts.policy.api.github_client", client_factory)
    monkeypatch.setenv("GITHUB_TOKEN", "github-token")
    monkeypatch.setenv("GH_TOKEN", "ignored-token")

    GitHubApi.from_env()

    client_factory.assert_called_once_with("github-token")


@pytest.mark.parametrize("token", [None, "", "   "])
def test_from_env_requires_github_token_without_fallback(monkeypatch, token) -> None:
    client_factory = Mock()
    monkeypatch.setattr("scripts.policy.api.github_client", client_factory)
    monkeypatch.setenv("GH_TOKEN", "ignored-token")
    if token is None:
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    else:
        monkeypatch.setenv("GITHUB_TOKEN", token)

    with pytest.raises(PolicyApiError, match="^GITHUB_TOKEN is required$"):
        GitHubApi.from_env()

    client_factory.assert_not_called()


class IncompleteRuns:
    totalCount = 2

    def __iter__(self):
        yield SimpleNamespace(
            id=100,
            path=WORKFLOW,
            event="pull_request",
            conclusion="success",
            actor=SimpleNamespace(
                id=1,
                login="octocat",
                type="User",
            ),
        )


class IncompleteWorkflow:
    def get_runs(self, *, created: str):
        assert created == "2026-09-27T00:00:00Z..2026-09-28T00:00:00Z"
        return IncompleteRuns()


def test_rejects_incomplete_run_history() -> None:
    with pytest.raises(PolicyApiError, match="read 1 of 2"):
        _read_workflow_window(IncompleteWorkflow(), WORKFLOW, SINCE, UNTIL)


def test_shared_adapter_reads_policies_and_history(monkeypatch) -> None:
    client = Mock()
    monkeypatch.setattr("scripts.policy.api.github_client", Mock(return_value=client))
    client.requester.requestJsonAndCheck.return_value = (
        {},
        {"total_count": 0, "policies": []},
    )
    observed = SimpleNamespace(
        id=100,
        path=WORKFLOW,
        event="pull_request",
        conclusion="success",
        actor=SimpleNamespace(id=1, login="octocat", type="User"),
    )
    paginated = Mock(totalCount=1)
    paginated.__iter__ = Mock(return_value=iter([observed]))
    repository = client.get_repo.return_value
    repository.get_workflow.return_value.get_runs.return_value = paginated
    repository.get_collaborators.return_value = [
        SimpleNamespace(id=1, role_name="write")
    ]
    client.get_organization.return_value.get_team.return_value.get_members.return_value = [
        SimpleNamespace(id=1)
    ]

    api = GitHubApi("test-token")
    path = "/repos/owner/repo/actions/policies?per_page=100&page=1&has_parents=false"
    assert api.list_policies("owner/repo") == ()
    runs = api.workflow_runs("owner/repo", WORKFLOW, SINCE, UNTIL)
    assert [(run.id, run.actor.login) for run in runs] == [(100, "octocat")]
    assert api.team_member_ids("owner/repo", 10) == frozenset({1})
    assert api.repository_roles("owner/repo") == {1: "write"}
    client.get_repo.assert_called_once_with("owner/repo")
    client.requester.requestJsonAndCheck.assert_called_once_with(
        "GET",
        path,
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
        },
    )


@pytest.mark.parametrize(
    "operation",
    [
        "list_policies",
        "get_policy",
        "workflow_runs",
        "team_member_ids",
        "repository_roles",
    ],
)
def test_shared_adapter_reports_github_errors(monkeypatch, operation) -> None:
    client = Mock()
    monkeypatch.setattr("scripts.policy.api.github_client", Mock(return_value=client))
    error = GithubException(403, {"message": "Forbidden"}, {})
    client.requester.requestJsonAndCheck.side_effect = error
    client.get_repo.side_effect = error
    client.get_organization.side_effect = error
    api = GitHubApi("test-token")
    arguments = {
        "list_policies": ("owner/repo",),
        "get_policy": ("owner/repo", PolicySummary(1, "NBS: example")),
        "workflow_runs": ("owner/repo", WORKFLOW, SINCE, UNTIL),
        "team_member_ids": ("owner/repo", 10),
        "repository_roles": ("owner/repo",),
    }

    with pytest.raises(PolicyApiError, match="403"):
        getattr(api, operation)(*arguments[operation])


@pytest.fixture
def policy_api(monkeypatch) -> tuple[GitHubApi, Mock]:
    client = Mock()
    monkeypatch.setattr("scripts.policy.api.github_client", Mock(return_value=client))
    return GitHubApi("test-token"), client.requester.requestJsonAndCheck


def live_policy() -> dict:
    return {
        "id": 1,
        "name": "NBS: example",
        "enforcement": "disabled",
        "target": "actions",
        "source_type": "Repository",
        "source": "owner/repo",
        "conditions": {"workflow_path": {"include": [WORKFLOW], "exclude": []}},
        "rules": [
            {
                "type": "restrict_action_events",
                "parameters": {"allowed_events": ["pull_request"]},
            }
        ],
    }


def test_reads_every_policy_page(policy_api) -> None:
    api, request = policy_api
    summaries = [{"id": index, "name": f"NBS: {index}"} for index in range(1, 102)]
    request.side_effect = [
        ({}, {"total_count": 101, "policies": summaries[:100]}),
        ({}, {"total_count": 101, "policies": summaries[100:]}),
    ]

    assert api.list_policies("owner/repo") == tuple(
        PolicySummary(item["id"], item["name"]) for item in summaries
    )
    assert [call.args[1] for call in request.call_args_list] == [
        f"/repos/owner/repo/actions/policies?per_page=100&page={page}&has_parents=false"
        for page in (1, 2)
    ]


@pytest.mark.parametrize(
    ("response", "message"),
    [
        ([], "not an object"),
        ({"total_count": -1, "policies": []}, "undocumented shape"),
        ({"total_count": True, "policies": []}, "undocumented shape"),
        ({"total_count": 1, "policies": {}}, "undocumented shape"),
        (
            {"total_count": 2, "policies": [{"id": 1, "name": "NBS: example"}]},
            "read 1 of 2",
        ),
        (
            {"total_count": 1, "policies": [{"id": True, "name": "NBS: example"}]},
            "undocumented shape",
        ),
        ({"total_count": 1, "policies": [{"id": 1}]}, "undocumented shape"),
        (
            {"total_count": 2, "policies": [{"id": 1, "name": "NBS: example"}] * 2},
            "duplicate IDs",
        ),
    ],
)
def test_rejects_invalid_policy_list(policy_api, response, message) -> None:
    api, request = policy_api
    request.return_value = ({}, response)

    with pytest.raises(PolicyApiError, match=message):
        api.list_policies("owner/repo")


def test_rejects_changed_policy_count(policy_api) -> None:
    api, request = policy_api
    request.side_effect = [
        (
            {},
            {
                "total_count": 101,
                "policies": [
                    {"id": index, "name": f"NBS: {index}"} for index in range(1, 101)
                ],
            },
        ),
        ({}, {"total_count": 100, "policies": []}),
    ]

    with pytest.raises(PolicyApiError, match="count changed"):
        api.list_policies("owner/repo")


def test_parses_full_policy_and_ignores_server_metadata(policy_api) -> None:
    api, request = policy_api
    request.return_value = ({}, live_policy())

    policy = api.get_policy("owner/repo", PolicySummary(1, "NBS: example"))

    assert policy.name == "NBS: example"
    assert policy.rules == (EventRule(("pull_request",)),)
    assert "id" not in policy.to_json()
    assert request.call_args.args[1] == "/repos/owner/repo/actions/policies/1"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("id", 2, "undocumented shape"),
        ("id", True, "undocumented shape"),
        ("name", "NBS: renamed", "undocumented shape"),
        (
            "rules",
            [{"type": "future_rule", "parameters": {}}],
            "not valid under any of the given schemas",
        ),
        (
            "rules",
            [{"type": "restrict_action_events", "parameters": None}],
            "not valid under any of the given schemas",
        ),
    ],
)
def test_rejects_malformed_full_policy(policy_api, field, value, message) -> None:
    api, request = policy_api
    body = live_policy()
    body[field] = value
    request.return_value = ({}, body)

    with pytest.raises(PolicyApiError, match=message):
        api.get_policy("owner/repo", PolicySummary(1, "NBS: example"))


def run_page(run_id: int, path: str) -> MagicMock:
    page = MagicMock(totalCount=1)
    page.__iter__.return_value = [
        SimpleNamespace(
            id=run_id,
            path=path,
            event="pull_request",
            conclusion="success",
            actor=SimpleNamespace(id=1, login="octocat", type="User"),
        )
    ]
    return page


def test_repository_history_discovers_unlisted_workflows(monkeypatch) -> None:
    client = Mock()
    monkeypatch.setattr("scripts.policy.api.github_client", Mock(return_value=client))
    new_path = ".github/workflows/new-or-deleted.yaml"
    repository = client.get_repo.return_value
    repository.get_workflow_runs.return_value = run_page(
        1, new_path + "@refs/heads/branch"
    )

    runs = GitHubApi("test-token").workflow_runs("owner/repo", None, SINCE, UNTIL)

    assert len(runs) == 1
    assert runs[0].workflow_path == new_path
    repository.get_workflow_runs.assert_called_once_with(
        created="2026-09-27T00:00:00Z..2026-09-28T00:00:00Z"
    )
    repository.get_workflow.assert_not_called()


@pytest.mark.parametrize("path", [None, WORKFLOW])
def test_run_search_splits_into_disjoint_whole_second_windows(
    monkeypatch, path
) -> None:
    monkeypatch.setattr("scripts.policy.api.RUN_SEARCH_LIMIT", 2)
    source = Mock()
    query = source.get_workflow_runs if path is None else source.get_runs
    query.side_effect = [
        SimpleNamespace(totalCount=2),
        run_page(1, WORKFLOW),
        run_page(2, WORKFLOW),
    ]
    runs = _read_workflow_window(source, path, SINCE, SINCE + timedelta(seconds=3))

    assert [run.id for run in runs] == [1, 2]
    assert [call.kwargs["created"] for call in query.call_args_list] == [
        "2026-09-27T00:00:00Z..2026-09-27T00:00:03Z",
        "2026-09-27T00:00:00Z..2026-09-27T00:00:01Z",
        "2026-09-27T00:00:02Z..2026-09-27T00:00:03Z",
    ]


def test_repository_history_rejects_a_saturated_second() -> None:
    repository = Mock()
    repository.get_workflow_runs.return_value = SimpleNamespace(totalCount=1000)
    with pytest.raises(PolicyApiError, match="in one second"):
        _read_workflow_window(repository, None, SINCE, SINCE)
    assert repository.get_workflow_runs.call_count == 1
