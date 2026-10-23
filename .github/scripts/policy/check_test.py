from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts.policy.api import GitHubApi
from scripts.policy.check import check_repository_policies
from scripts.policy.types import (
    EventRule,
    Policy,
    PolicyCheckError,
    PolicyFile,
    PolicySummary,
)


class FakePolicySource:
    def __init__(
        self,
        summaries: tuple[PolicySummary, ...],
        policies: dict[int, Policy] | None = None,
    ) -> None:
        self.summaries = summaries
        self.policies = policies or {}
        self.calls: list[PolicySummary] = []

    def list_policies(self, repository: str) -> tuple[PolicySummary, ...]:
        assert repository == "ydb-platform/nbs"
        return self.summaries

    def get_policy(self, repository: str, summary: PolicySummary) -> Policy:
        assert repository == "ydb-platform/nbs"
        self.calls.append(summary)
        return self.policies[summary.id]


def example_policy() -> Policy:
    return Policy(
        name="NBS: example",
        enforcement="disabled",
        workflow_paths=frozenset({".github/workflows/example.yaml"}),
        excluded_workflow_paths=frozenset(),
        rules=(EventRule(("pull_request",)),),
    )


def desired_policy() -> tuple[PolicyFile, ...]:
    return (PolicyFile(Path("example.json"), example_policy()),)


def test_in_sync_uses_full_policy_read() -> None:
    summary = PolicySummary(1, "NBS: example")
    api = FakePolicySource((summary,), {1: example_policy()})

    result = check_repository_policies(api, "ydb-platform/nbs", desired_policy())

    assert result.ok
    assert result.lines == ("IN-SYNC  example.json",)
    assert api.calls == [summary]


def test_reports_missing_and_unmanaged_policies() -> None:
    api = FakePolicySource((PolicySummary(9, "Manual policy"),))

    result = check_repository_policies(api, "ydb-platform/nbs", desired_policy())

    assert not result.ok
    assert "DRIFT    example.json: live policy is missing" in result.lines
    assert "UNMANAGED Manual policy (id 9): ignored" in result.lines
    assert not api.calls


def test_reports_changed_and_stale_managed_policies() -> None:
    changed = replace(example_policy(), enforcement="active")
    stale = replace(example_policy(), name="NBS: stale")
    api = FakePolicySource(
        (PolicySummary(1, changed.name), PolicySummary(2, stale.name)),
        {1: changed, 2: stale},
    )

    result = check_repository_policies(api, "ydb-platform/nbs", desired_policy())

    assert not result.ok
    assert any("live policy differs" in line for line in result.lines)
    assert '          want: {"conditions":' in "\n".join(result.lines)
    assert any(line.startswith("STALE    NBS: stale") for line in result.lines)


def test_reports_rule_drift() -> None:
    changed = replace(example_policy(), rules=(EventRule(("push",)),))
    api = FakePolicySource((PolicySummary(1, changed.name),), {1: changed})

    result = check_repository_policies(api, "ydb-platform/nbs", desired_policy())

    assert not result.ok
    assert result.lines[0] == "DRIFT    example.json: live policy differs"
    assert '"pull_request"' in result.lines[1]
    assert '"push"' in result.lines[2]


def test_rejects_duplicate_live_managed_names() -> None:
    api = FakePolicySource(
        (PolicySummary(1, "NBS: example"), PolicySummary(2, "NBS: example"))
    )

    with pytest.raises(PolicyCheckError, match="duplicate live managed"):
        check_repository_policies(api, "ydb-platform/nbs", desired_policy())
    assert not api.calls


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("rules",), []),
        (("rules",), "omitted"),
        (("conditions",), None),
        (("conditions",), {}),
        (("conditions",), "omitted"),
        (("conditions", "workflow_path", "include"), []),
        (("rules", 0, "parameters"), "omitted"),
        (("rules", 0, "parameters", "allowed_events"), []),
    ],
)
def test_github_valid_drift_does_not_abort_other_results(
    monkeypatch, path, value
) -> None:
    desired = desired_policy()
    body = desired[0].policy.to_json() | {
        "id": 1,
        "target": "actions",
        "source_type": "Repository",
        "source": "ydb-platform/nbs",
    }
    target = body
    for part in path[:-1]:
        target = target[part]
    if value == "omitted":
        del target[path[-1]]
    else:
        target[path[-1]] = value

    client = Mock()
    monkeypatch.setattr("scripts.policy.api.github_client", Mock(return_value=client))
    client.requester.requestJsonAndCheck.side_effect = [
        (
            {},
            {
                "total_count": 2,
                "policies": [
                    {"id": 1, "name": desired[0].name},
                    {"id": 2, "name": "NBS: stale"},
                ],
            },
        ),
        ({}, body),
    ]
    missing = PolicyFile(
        Path("missing.json"), replace(example_policy(), name="NBS: missing")
    )

    result = check_repository_policies(
        GitHubApi("test-token"), "ydb-platform/nbs", (*desired, missing)
    )

    assert not result.ok
    assert result.lines[0] == "DRIFT    example.json: live policy differs"
    assert "DRIFT    missing.json: live policy is missing" in result.lines
    assert "STALE    NBS: stale (id 2): no policy file owns it" in result.lines
    assert client.requester.requestJsonAndCheck.call_count == 2
