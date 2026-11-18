from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts.policy.model import load_policies
from scripts.policy.simulate import _parse_time, main, simulate_repository_policies
from scripts.policy.types import (
    Actor,
    ActorRule,
    ActorSelector,
    EventRule,
    ObservedRun,
    Policy,
    PolicyFile,
    PolicySimulationError,
)

UTC = timezone.utc
REPOSITORY = "ydb-platform/nbs"
WORKFLOW = ".github/workflows/example.yaml"
SINCE = datetime(2026, 9, 27, tzinfo=UTC)
UNTIL = datetime(2026, 9, 28, tzinfo=UTC)


@pytest.mark.parametrize("hours", [0, 24, 48])
def test_since_accepts_negative_whole_hours(hours: int) -> None:
    assert _parse_time(f"-{hours}h", "--since", relative_to=UNTIL) == (
        UNTIL - timedelta(hours=hours)
    )


@pytest.mark.parametrize("relative_to", [None, UNTIL])
def test_parse_time_accepts_timezone_qualified_iso_timestamp(relative_to) -> None:
    assert (
        _parse_time(
            "2026-09-27T02:00:00.123456+02:00", "--since", relative_to=relative_to
        )
        == SINCE
    )


@pytest.mark.parametrize("value", ["24h", "-1.5h", "-24m", "-999999999999h"])
def test_since_rejects_invalid_or_out_of_range_offsets(value: str) -> None:
    with pytest.raises(PolicySimulationError, match="negative whole hours"):
        _parse_time(value, "--since", relative_to=UNTIL)


def test_until_requires_an_absolute_timestamp() -> None:
    with pytest.raises(PolicySimulationError, match="ISO-8601"):
        _parse_time("-24h", "--until")


def test_parse_time_requires_a_timezone() -> None:
    with pytest.raises(PolicySimulationError, match="must include a timezone"):
        _parse_time("2026-09-27T00:00:00", "--since", relative_to=UNTIL)


class FakeHistory:
    def __init__(
        self,
        runs: tuple[ObservedRun, ...],
        *,
        teams: dict[int, frozenset[int]] | None = None,
        roles: dict[int, str] | None = None,
    ) -> None:
        self.runs = runs
        self.teams = teams or {}
        self.roles = roles or {}

    def workflow_runs(
        self,
        repository: str,
        workflow_path: str | None,
        since: datetime,
        until: datetime,
    ) -> tuple[ObservedRun, ...]:
        assert repository == REPOSITORY
        assert workflow_path in {None, WORKFLOW}
        assert since == SINCE
        assert until == UNTIL
        return tuple(
            run
            for run in self.runs
            if workflow_path is None or run.workflow_path == workflow_path
        )

    def team_member_ids(self, repository: str, team_id: int) -> frozenset[int]:
        assert repository == REPOSITORY
        return self.teams[team_id]

    def repository_roles(self, repository: str) -> dict[int, str]:
        assert repository == REPOSITORY
        return dict(self.roles)


def policy(
    actor_type: str,
    actor_id: int,
    event: str = "pull_request",
    *,
    workflow_path: str = WORKFLOW,
) -> PolicyFile:
    return PolicyFile(
        Path("example.json"),
        Policy(
            name="NBS: example",
            enforcement="disabled",
            workflow_paths=frozenset({workflow_path}),
            excluded_workflow_paths=frozenset(),
            rules=(
                ActorRule((ActorSelector(id=actor_id, type=actor_type),)),
                EventRule((event,)),
            ),
        ),
    )


def run(
    actor_id: int = 1,
    event: str = "pull_request",
    conclusion: str = "success",
) -> ObservedRun:
    return ObservedRun(
        id=100,
        workflow_path=WORKFLOW,
        event=event,
        actor=Actor(id=actor_id, login="octocat", type="User"),
        conclusion=conclusion,
    )


@pytest.mark.parametrize("since_args", [[], ["--since=-24h"]])
def test_cli_lookback_is_relative_to_until(monkeypatch, since_args) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "simulate",
            "--repository",
            REPOSITORY,
            "--until=2026-09-28T00:00:00Z",
            *since_args,
        ],
    )
    monkeypatch.setattr(
        "scripts.policy.simulate.load_policies", Mock(return_value=(policy("User", 1),))
    )
    monkeypatch.setattr(
        "scripts.policy.simulate.GitHubApi.from_env", lambda: FakeHistory((run(),))
    )

    assert main() == 0


def test_allows_current_team_member() -> None:
    history = FakeHistory(
        (run(),),
        teams={10: frozenset({1})},
    )

    result = simulate_repository_policies(
        history,
        REPOSITORY,
        (policy("Team", 10),),
        SINCE,
        UNTIL,
        show_allowed=True,
    )

    assert result.ok
    assert result.allowed == 1
    assert result.denied == 0
    assert any(line.startswith("ALLOW") for line in result.lines)


def test_denies_disallowed_actor_and_event() -> None:
    history = FakeHistory((run(event="workflow_dispatch", conclusion="skipped"),))

    result = simulate_repository_policies(
        history,
        REPOSITORY,
        (policy("User", 2),),
        SINCE,
        UNTIL,
    )

    assert not result.ok
    assert result.denied == 1
    assert result.denied_skipped == 1
    denial = next(line for line in result.lines if line.startswith("DENY"))
    assert "observed_conclusion=skipped" in denial
    assert "actor is not allowed" in denial
    assert "event 'workflow_dispatch' is not allowed" in denial
    assert (
        f"          run=100 url=https://github.com/{REPOSITORY}/actions/runs/100"
        in result.lines
    )


@pytest.mark.parametrize("show_allowed", [False, True])
def test_grouped_results_link_every_run_without_changing_counts(show_allowed) -> None:
    history = FakeHistory(
        (
            replace(run(), id=900),
            replace(run(actor_id=2), id=200),
            run(),
        )
    )
    result = simulate_repository_policies(
        history,
        REPOSITORY,
        (policy("User", 2),),
        SINCE,
        UNTIL,
        show_allowed=show_allowed,
    )

    assert result.denied == 2
    assert result.allowed == 1
    denial_index = next(
        index for index, line in enumerate(result.lines) if line.startswith("DENY")
    )
    assert "count=2 " in result.lines[denial_index]
    details_start = denial_index + 1
    details_end = denial_index + 3
    assert result.lines[details_start:details_end] == (
        f"          run=100 url=https://github.com/{REPOSITORY}/actions/runs/100",
        f"          run=900 url=https://github.com/{REPOSITORY}/actions/runs/900",
    )
    allowed_link = (
        f"          run=200 url=https://github.com/{REPOSITORY}/actions/runs/200"
    )
    assert (allowed_link in result.lines) is show_allowed
    assert sum(line.startswith("          run=") for line in result.lines) == 2 + int(
        show_allowed
    )


def test_allows_current_repository_role() -> None:
    history = FakeHistory((run(),), roles={1: "admin"})

    result = simulate_repository_policies(
        history,
        REPOSITORY,
        (policy("RepositoryRole", 5),),
        SINCE,
        UNTIL,
    )

    assert result.ok
    assert result.allowed == 1


@pytest.mark.parametrize(
    ("actor_type", "actor_id", "message"),
    [
        ("BusinessTeam", 1, "simulation does not support actor types"),
        ("EnterpriseTeam", 1, "simulation does not support actor types"),
        ("App", 29110, "simulation does not support actor types"),
        ("IntegrationInstallation", 1, "simulation does not support actor types"),
        ("RepositoryRole", 99, "unknown RepositoryRole IDs"),
    ],
)
def test_rejects_unsupported_actors_even_without_runs(
    actor_type, actor_id, message
) -> None:
    with pytest.raises(PolicySimulationError, match=message):
        simulate_repository_policies(
            FakeHistory(()),
            REPOSITORY,
            (policy(actor_type, actor_id),),
            SINCE,
            UNTIL,
        )


@pytest.mark.parametrize("reverse", [False, True])
def test_overlapping_policies_intersect_regardless_of_order(reverse) -> None:
    restricted = policy("User", 2)
    allowed = replace(
        policy("User", 1), policy=replace(policy("User", 1).policy, name="NBS: allowed")
    )
    policies = (restricted, allowed)
    if reverse:
        policies = tuple(reversed(policies))
    result = simulate_repository_policies(
        FakeHistory((run(),)), REPOSITORY, policies, SINCE, UNTIL
    )
    assert result.denied == 1
    assert result.allowed == 0


def test_catch_all_checks_new_paths_without_applying_to_exceptions() -> None:
    specific = policy("User", 1)
    catch_all = replace(
        policy("User", 2).policy,
        name="NBS: default",
        workflow_paths=frozenset({"~ALL"}),
        excluded_workflow_paths=frozenset({WORKFLOW}),
    )
    renamed = replace(
        run(), id=101, workflow_path=".github/workflows/new-or-renamed.yaml"
    )
    result = simulate_repository_policies(
        FakeHistory((run(), renamed)),
        REPOSITORY,
        (PolicyFile(Path("default.json"), catch_all), specific),
        SINCE,
        UNTIL,
    )
    assert result.allowed == 1
    assert result.denied == 1
    denial = next(line for line in result.lines if line.startswith("DENY"))
    assert renamed.workflow_path in denial
    assert "NBS: default: actor is not allowed" in denial


@pytest.mark.parametrize("show_allowed", [False, True])
@pytest.mark.parametrize(
    "workflow_path",
    [
        "dynamic/dependabot/dependabot-updates",
        "dynamic/agents/copilot-pull-request-reviewer",
    ],
)
def test_builtin_runs_are_reported_separately_with_links(
    workflow_path: str, show_allowed: bool
) -> None:
    observed = replace(
        run(event="dynamic"),
        workflow_path=workflow_path,
        actor=Actor(49699333, "dependabot[bot]", "Bot"),
    )
    result = simulate_repository_policies(
        FakeHistory((observed, replace(observed, id=101))),
        REPOSITORY,
        (policy("User", 1, "dynamic", workflow_path="~ALL"),),
        SINCE,
        UNTIL,
        show_allowed=show_allowed,
    )

    assert result.ok
    assert result.exempt == 2
    assert result.allowed == result.denied == result.denied_skipped == 0
    exemption = next(line for line in result.lines if line.startswith("EXEMPT"))
    assert "count=2 " in exemption
    assert workflow_path in exemption
    assert "actor restrictions do not apply" in exemption
    assert not any(line.startswith(("ALLOW", "DENY")) for line in result.lines)
    assert result.lines[-1] == (
        "SUMMARY allowed=0 denied=0 denied_skipped=0 exempt=2 no_runs=0"
    )
    for run_id in (100, 101):
        assert (
            f"          run={run_id} url=https://github.com/{REPOSITORY}/actions/runs/{run_id}"
            in result.lines
        )


@pytest.mark.parametrize(
    "workflow_path",
    [
        "dynamic/dependabot/dependabot-updates",
        "dynamic/agents/copilot-pull-request-reviewer",
    ],
)
def test_builtin_event_violations_still_fail_simulation(workflow_path: str) -> None:
    observed = replace(run(event="dynamic"), workflow_path=workflow_path)
    result = simulate_repository_policies(
        FakeHistory((observed,)),
        REPOSITORY,
        (policy("User", 2, workflow_path="~ALL"),),
        SINCE,
        UNTIL,
    )

    assert not result.ok
    assert result.denied == 1
    assert result.exempt == result.allowed == 0
    denial = next(line for line in result.lines if line.startswith("DENY"))
    assert "event 'dynamic' is not allowed" in denial
    assert "actor is not allowed" not in denial


def test_builtin_exemption_does_not_hide_denials_for_repository_bot_workflows() -> None:
    bot = Actor(49699333, "dependabot[bot]", "Bot")
    builtin = replace(
        run(),
        id=101,
        workflow_path="dynamic/dependabot/dependabot-updates",
        actor=bot,
    )
    repository_bot_run = replace(run(), id=102, actor=bot)
    result = simulate_repository_policies(
        FakeHistory((run(), builtin, repository_bot_run)),
        REPOSITORY,
        (policy("User", 1, workflow_path="~ALL"),),
        SINCE,
        UNTIL,
    )

    assert not result.ok
    assert result.allowed == result.denied == result.exempt == 1
    denial = next(line for line in result.lines if line.startswith("DENY"))
    assert WORKFLOW in denial
    assert "actor is not allowed" in denial
    assert result.lines[-1] == (
        "SUMMARY allowed=1 denied=1 denied_skipped=0 exempt=1 no_runs=0"
    )


@pytest.mark.parametrize(
    ("workflow", "event", "actor_id", "allowed"),
    [
        ("pr-github-actions.yaml", "pull_request", 1, False),
        ("pr-github-actions.yaml", "pull_request", 2, True),
        ("pr-vscode-yatool-extension.yaml", "pull_request", 1, True),
        ("new-or-renamed.yaml", "pull_request", 1, True),
        ("new-or-renamed.yaml", "pull_request", 2, True),
        ("nightly.yaml", "schedule", 1, True),
        ("nightly.yaml", "schedule", 2, True),
        ("packer.yaml", "workflow_dispatch", 1, True),
        ("new-or-renamed.yaml", "pull_request", 3, False),
        ("new-or-renamed.yaml", "pull_request", 4, True),
        ("new-or-renamed.yaml", "pull_request", 5, True),
        ("nightly.yaml", "schedule", 6, True),
        ("nightly.yaml", "schedule", 7, True),
        ("pr-github-actions.yaml", "pull_request", 4, False),
        ("pr.yaml", "pull_request_target", 1, True),
        ("pre-commit.yaml", "pull_request_target", 1, True),
        ("pr.yaml", "pull_request_target", 3, False),
        ("check-pr-source.yaml", "pull_request_target", 3, True),
        ("approvals.yaml", "issue_comment", 1, False),
        ("approvals.yaml", "pull_request_review", 2, False),
        ("approvals.yaml", "pull_request_review", 4, True),
        ("approvals.yaml", "issue_comment", 5, True),
        ("approvals.yaml", "issue_comment", 2557343, True),
        ("approvals.yaml", "pull_request_review", 127485837, True),
        ("approvals.yaml", "pull_request", 4, False),
        ("approvals.yaml", "workflow_dispatch", 2557343, False),
        ("approvals.yaml", "issue_comment", 274634113, False),
    ]
    + [
        (workflow, event, 1, False)
        for workflow in ("pr.yaml", "pre-commit.yaml", "check-pr-source.yaml")
        for event in (
            "pull_request",
            "pull_request_review",
            "pull_request_review_comment",
            "workflow_dispatch",
        )
    ],
)
def test_repository_policy_security_boundary(
    workflow, event, actor_id, allowed
) -> None:
    observed = replace(
        run(actor_id, event), workflow_path=f".github/workflows/{workflow}"
    )
    history = FakeHistory(
        (observed,),
        teams={
            19724215: frozenset({2}),
            9863268: frozenset({4}),
            9863273: frozenset({5}),
        },
        roles={1: "write", 2: "write", 6: "maintain", 7: "admin"},
    )
    result = simulate_repository_policies(
        history, REPOSITORY, load_policies(), SINCE, UNTIL
    )
    assert result.ok is allowed
    assert result.allowed == int(allowed)
    assert result.denied == int(not allowed)
