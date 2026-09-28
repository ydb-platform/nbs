from dataclasses import replace

import pytest

from scripts.policy.types import (
    Actor,
    ActorEvidence,
    ActorRule,
    ActorSelector,
    EventRule,
    ObservedRun,
    Policy,
    PolicySimulationError,
)

RUN = ObservedRun(
    id=100,
    workflow_path=".github/workflows/example.yaml",
    event="pull_request",
    actor=Actor(id=1, login="octocat", type="User"),
    conclusion="success",
)
EVIDENCE = ActorEvidence(
    team_members={10: frozenset({1})},
    repository_roles={1: "write"},
)


@pytest.mark.parametrize("actor_type", ["User", "Bot"])
def test_selector_matches_both_actor_type_and_id(actor_type: str) -> None:
    selector = ActorSelector(id=1, type=actor_type)
    actor = Actor(id=1, login="example", type=actor_type)

    assert selector.matches(actor, EVIDENCE)
    assert not selector.matches(replace(actor, id=2), EVIDENCE)
    assert not selector.matches(replace(actor, type="Team"), EVIDENCE)


@pytest.mark.parametrize(
    ("selector", "expected"),
    [
        (ActorSelector(10, "Team"), True),
        (ActorSelector(11, "Team"), False),
        (ActorSelector(4, "RepositoryRole"), True),
        (ActorSelector(2, "RepositoryRole"), False),
        (ActorSelector(5, "RepositoryRole"), False),
    ],
)
def test_selector_uses_membership_evidence(
    selector: ActorSelector, expected: bool
) -> None:
    assert selector.matches(RUN.actor, EVIDENCE) is expected
    assert not selector.matches(RUN.actor, ActorEvidence({}, {}))


@pytest.mark.parametrize(
    "selector",
    [
        ActorSelector(1, "BusinessTeam"),
        ActorSelector(1, "EnterpriseTeam"),
        ActorSelector(29110, "App"),
        ActorSelector(1, "IntegrationInstallation"),
        ActorSelector(99, "RepositoryRole"),
    ],
)
def test_selector_rejects_unsupported_simulation(selector: ActorSelector) -> None:
    with pytest.raises(PolicySimulationError):
        selector.matches(RUN.actor, EVIDENCE)


def test_dependabot_app_is_not_confused_with_its_bot_user() -> None:
    dependabot = Actor(49699333, "dependabot[bot]", "Bot")
    with pytest.raises(PolicySimulationError, match="App"):
        ActorSelector(29110, "App").matches(dependabot, EVIDENCE)
    assert ActorSelector(49699333, "Bot").matches(dependabot, EVIDENCE)


def test_rules_allow_any_matching_actor_but_require_every_rule() -> None:
    policy = Policy(
        name="NBS: example",
        enforcement="disabled",
        workflow_paths=frozenset({RUN.workflow_path}),
        excluded_workflow_paths=frozenset(),
        rules=(
            ActorRule((ActorSelector(2, "User"), ActorSelector(10, "Team"))),
            EventRule(("pull_request",)),
        ),
    )

    assert policy.violations(RUN, EVIDENCE) == ()
    assert policy.violations(replace(RUN, event="push"), EVIDENCE) == (
        "event 'push' is not allowed",
    )
    assert policy.violations(replace(RUN, event="push"), ActorEvidence({}, {})) == (
        "actor is not allowed",
        "event 'push' is not allowed",
    )


def test_missing_actor_rule_does_not_restrict_actors() -> None:
    policy = Policy(
        name="NBS: example",
        enforcement="disabled",
        workflow_paths=frozenset({RUN.workflow_path}),
        excluded_workflow_paths=frozenset(),
        rules=(EventRule(("pull_request",)),),
    )

    assert policy.allowed_actors == ()
    assert policy.violations(RUN, ActorEvidence({}, {})) == ()


def test_canonical_json_ignores_order_without_mutating_rules() -> None:
    actors = (
        ActorSelector(9, "User"),
        ActorSelector(1, "Bot"),
        ActorSelector(2, "User"),
    )
    events = ("workflow_dispatch", "pull_request")
    policy = Policy(
        name="NBS: example",
        enforcement="disabled",
        workflow_paths=frozenset({".github/workflows/b.yaml", RUN.workflow_path}),
        excluded_workflow_paths=frozenset(),
        rules=(ActorRule(actors), EventRule(events)),
    )
    reordered = replace(
        policy,
        rules=(EventRule(tuple(reversed(events))), ActorRule(tuple(reversed(actors)))),
    )

    assert policy.to_json() == reordered.to_json()
    assert policy.allowed_actors == actors
    rules = policy.to_json()["rules"]
    assert rules[0]["parameters"]["allowed_events"] == [
        "pull_request",
        "workflow_dispatch",
    ]
    assert rules[1]["parameters"]["allowed_actors"] == [
        {"id": 1, "type": "Bot"},
        {"id": 2, "type": "User"},
        {"id": 9, "type": "User"},
    ]


def test_catch_all_matches_new_paths_but_not_exclusions() -> None:
    policy = Policy(
        name="NBS: default",
        enforcement="disabled",
        workflow_paths=frozenset({"~ALL"}),
        excluded_workflow_paths=frozenset({RUN.workflow_path}),
        rules=(),
    )
    assert policy.targets_all_workflows
    assert policy.matches_workflow(".github/workflows/new-or-renamed.yaml")
    assert not policy.matches_workflow(RUN.workflow_path)
