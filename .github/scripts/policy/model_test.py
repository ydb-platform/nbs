from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from scripts.policy.model import (
    PolicyValidator,
    default_policy_dir,
    default_repo_root,
    load_policies,
)
from scripts.policy.schema import policy_schema
from scripts.policy.types import (
    ActorRule,
    ActorSelector,
    EventRule,
    PolicyValidationError,
)

WORKFLOW = ".github/workflows/example.yaml"


def base_policy(*, local: bool = True) -> dict:
    body = {
        "name": "NBS: example",
        "enforcement": "disabled",
        "conditions": {"workflow_path": {"include": [WORKFLOW], "exclude": []}},
        "rules": [
            {
                "type": "restrict_action_events",
                "parameters": {"allowed_events": ["pull_request"]},
            },
            {
                "type": "restrict_actions_actors",
                "parameters": {"allowed_actors": [{"id": 123, "type": "User"}]},
            },
        ],
    }
    if not local:
        body.update(
            id=42, target="actions", source_type="Repository", source="ydb-platform/nbs"
        )
    return body


def write_fixture(
    tmp_path: Path,
    policies: list[dict],
) -> tuple[Path, Path]:
    workflow_path = tmp_path / WORKFLOW
    workflow_path.parent.mkdir(parents=True)
    workflow_path.write_text("name: example\non: push\njobs: {}\n", encoding="utf-8")
    policy_dir = tmp_path / ".github" / "policy"
    policy_dir.mkdir()
    for index, policy in enumerate(policies):
        (policy_dir / f"policy-{index}.json").write_text(
            json.dumps(policy), encoding="utf-8"
        )
    return policy_dir, tmp_path


def validation_errors(policy_dir: Path, repo_root: Path) -> tuple[str, ...]:
    with pytest.raises(PolicyValidationError) as error:
        load_policies(policy_dir, repo_root)
    return error.value.errors


def test_repository_policies_validate() -> None:
    policies = load_policies(default_policy_dir(), default_repo_root())
    assert {policy.path.name for policy in policies} == {
        "approvals.json",
        "check-pr-source.json",
        "default-workflow-actors.json",
        "pr-github-actions.json",
        "pull-request-target-ci.json",
    }


def test_catch_all_is_ci_roles_plus_the_three_requested_teams() -> None:
    policies = {item.path.name: item.policy for item in load_policies()}
    ci_actors = set(policies["pull-request-target-ci.json"].allowed_actors)
    assert set(policies["default-workflow-actors.json"].allowed_actors) == ci_actors | {
        ActorSelector(9863268, "Team"),
        ActorSelector(9863273, "Team"),
        ActorSelector(19724215, "Team"),
    }


def test_approval_policy_has_a_separate_narrow_actor_allowlist() -> None:
    policies = {item.path.name: item.policy for item in load_policies()}
    assert set(policies["approvals.json"].allowed_actors) == {
        ActorSelector(9863268, "Team"),
        ActorSelector(9863273, "Team"),
        ActorSelector(2557343, "User"),
        ActorSelector(127485837, "User"),
    }
    assert (
        ".github/workflows/approvals.yaml"
        in policies["default-workflow-actors.json"].excluded_workflow_paths
    )


def test_rejects_unknown_nested_key(tmp_path: Path) -> None:
    policy = base_policy()
    policy["rules"][0]["parameters"]["allowed_event"] = "push"
    policy_dir, repo_root = write_fixture(tmp_path, [policy])

    errors = validation_errors(policy_dir, repo_root)

    assert any("'allowed_event' was unexpected" in error for error in errors)


@pytest.mark.parametrize(
    ("field", "value", "fragment"),
    [
        ("rules", None, "rules: None is not of type 'array'"),
        ("conditions", None, "conditions: None is not of type 'object'"),
    ],
)
def test_malformed_policy_shape_is_reported(
    tmp_path: Path, field: str, value: object, fragment: str
) -> None:
    policy = base_policy()
    policy[field] = value
    policy_dir, repo_root = write_fixture(tmp_path, [policy])

    errors = validation_errors(policy_dir, repo_root)

    assert any(fragment in error for error in errors)


def test_unhashable_rule_type_is_reported(tmp_path: Path) -> None:
    policy = base_policy()
    policy["rules"][0]["type"] = []
    policy_dir, repo_root = write_fixture(tmp_path, [policy])

    errors = validation_errors(policy_dir, repo_root)

    assert any("rules[0].type: [] is not one of" in error for error in errors)


def test_unhashable_event_is_reported(tmp_path: Path) -> None:
    policy = base_policy()
    policy["rules"][0]["parameters"]["allowed_events"] = [{"pull_request_target": True}]
    policy_dir, repo_root = write_fixture(tmp_path, [policy])

    errors = validation_errors(policy_dir, repo_root)

    assert any("is not of type 'string'" in error for error in errors)


def test_unhashable_workflow_path_is_reported(tmp_path: Path) -> None:
    policy = base_policy()
    policy["conditions"]["workflow_path"]["include"] = [{"path": WORKFLOW}]
    policy_dir, repo_root = write_fixture(tmp_path, [policy])

    errors = validation_errors(policy_dir, repo_root)

    assert any("is not of type 'string'" in error for error in errors)


@pytest.mark.parametrize("enforcement", ["evaluate", "enforce", None, {}])
def test_rejects_unsupported_enforcement(tmp_path: Path, enforcement: object) -> None:
    policy = base_policy()
    policy["enforcement"] = enforcement
    policy_dir, repo_root = write_fixture(tmp_path, [policy])

    errors = validation_errors(policy_dir, repo_root)

    assert any("is not one of ['active', 'disabled']" in error for error in errors)


@pytest.mark.parametrize(
    "actor, fragment",
    [
        ({"id": True, "type": "User"}, "is not of type 'integer'"),
        ({"id": 1.0, "type": "User"}, "is not of type 'integer'"),
        ({"id": 1.5, "type": "User"}, "is not of type 'integer'"),
        ({"id": 0, "type": "User"}, "less than the minimum of 1"),
        ({"id": -1, "type": "User"}, "less than the minimum of 1"),
        ({"id": 1, "type": "Login"}, "is not one of"),
        ({"id": 1, "type": "User", "login": "octocat"}, "'login' was unexpected"),
        ({"id": 1, "type": []}, "is not one of"),
    ],
)
def test_rejects_invalid_actor(tmp_path: Path, actor: dict, fragment: str) -> None:
    policy = base_policy()
    policy["rules"][1]["parameters"]["allowed_actors"] = [actor]
    policy_dir, repo_root = write_fixture(tmp_path, [policy])

    errors = validation_errors(policy_dir, repo_root)

    assert any(fragment in error for error in errors)


def test_accepts_overlapping_policy_scopes_for_intersection(tmp_path: Path) -> None:
    first = base_policy()
    second = copy.deepcopy(first)
    second["name"] = "NBS: another policy"
    policy_dir, repo_root = write_fixture(tmp_path, [first, second])

    assert len(load_policies(policy_dir, repo_root)) == 2


def test_normalization_ignores_order_and_server_fields() -> None:
    desired = base_policy()
    live = base_policy(local=False)
    live["rules"].reverse()
    live["rules"][0]["parameters"]["allowed_actors"][0]["login"] = "octocat"

    assert (
        PolicyValidator("live", local=False).parse(live).to_json()
        == PolicyValidator("desired").parse(desired).to_json()
    )


def test_parses_typed_rules_without_changing_input() -> None:
    body = base_policy()
    original = copy.deepcopy(body)

    policy = PolicyValidator("example.json").parse(body)

    assert policy.rules == (
        EventRule(("pull_request",)),
        ActorRule((ActorSelector(123, "User"),)),
    )
    assert policy.workflow_paths == frozenset({WORKFLOW})
    assert body == original
    assert PolicyValidator("roundtrip").parse(policy.to_json()) == policy


def test_validator_accumulates_errors_and_resets_between_parses() -> None:
    body = base_policy()
    body["conditions"] = None
    body["rules"] = None
    validator = PolicyValidator("example.json")

    with pytest.raises(PolicyValidationError) as raised:
        validator.parse(body)

    reported = raised.value.errors
    assert {
        "example.json:conditions: None is not of type 'object'",
        "example.json:rules: None is not of type 'array'",
    }.issubset(reported)
    assert len(reported) == len(set(reported))
    assert validator.parse(base_policy()).name == "NBS: example"
    assert not validator.errors
    assert raised.value.errors == reported


def test_live_parser_preserves_nonlocal_values_for_drift_detection() -> None:
    body = base_policy(local=False)
    body["name"] = "Manually managed"
    body["enforcement"] = "evaluate"
    body["conditions"]["workflow_path"] = {"include": ["**"], "exclude": [WORKFLOW]}

    policy = PolicyValidator("live", local=False).parse(body)

    assert policy.to_json() == {key: body[key] for key in base_policy()}


@pytest.mark.parametrize("field", ["allowed_events", "allowed_actors"])
def test_rejects_duplicate_rule_values(field: str) -> None:
    body = base_policy()
    rule = body["rules"][0 if field == "allowed_events" else 1]
    rule["parameters"][field] *= 2

    with pytest.raises(PolicyValidationError, match="non-unique elements"):
        PolicyValidator("example.json").parse(body)


@pytest.mark.parametrize("local", [True, False])
def test_policy_schema_is_valid_and_json_serializable(local: bool) -> None:
    schema = policy_schema(local=local)

    Draft202012Validator.check_schema(schema)
    assert json.loads(json.dumps(schema)) == schema


@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize(
    "path",
    [
        (),
        ("conditions",),
        ("conditions", "workflow_path"),
        ("rules", 0),
        ("rules", 0, "parameters"),
        ("rules", 1, "parameters"),
        ("rules", 1, "parameters", "allowed_actors", 0),
    ],
)
def test_metadata_is_only_allowed_in_live_responses(local, path) -> None:
    body = base_policy(local=local)
    target = body
    for part in path:
        target = target[part]
    target["server_metadata"] = {"id": 42}
    validator = PolicyValidator("example.json", local=local)

    if local:
        with pytest.raises(
            PolicyValidationError, match="'server_metadata' was unexpected"
        ):
            validator.parse(body)
    elif path == ("conditions",):
        # GitHub's repository conditions allow only the workflow_path key.
        with pytest.raises(PolicyValidationError, match="conditions:"):
            validator.parse(body)
    else:
        assert validator.parse(body) == validator.parse(base_policy(local=False))


@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize(
    ("path", "key"),
    [
        ((), "name"),
        ((), "enforcement"),
        ((), "conditions"),
        ((), "rules"),
        (("conditions",), "workflow_path"),
        (("conditions", "workflow_path"), "include"),
        (("conditions", "workflow_path"), "exclude"),
        (("rules", 0), "type"),
        (("rules", 0), "parameters"),
        (("rules", 0, "parameters"), "allowed_events"),
        (("rules", 1, "parameters"), "allowed_actors"),
        (("rules", 1, "parameters", "allowed_actors", 0), "id"),
        (("rules", 1, "parameters", "allowed_actors", 0), "type"),
    ],
)
def test_required_fields_cannot_be_omitted(local, path, key) -> None:
    body = base_policy(local=local)
    target = body
    for part in path:
        target = target[part]
    del target[key]

    if not local and key in {"conditions", "rules", "workflow_path", "parameters"}:
        assert PolicyValidator("live", local=False).parse(body) != PolicyValidator(
            "local"
        ).parse(base_policy())
        return

    with pytest.raises(PolicyValidationError):
        PolicyValidator("example.json", local=local).parse(body)


@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize("rule_index", [0, 1])
def test_rejects_duplicate_rule_types_with_different_values(local, rule_index) -> None:
    body = base_policy(local=local)
    extra = copy.deepcopy(body["rules"][rule_index])
    if rule_index == 0:
        extra["parameters"]["allowed_events"] = ["push"]
    else:
        extra["parameters"]["allowed_actors"][0]["id"] = 456
    body["rules"].append(extra)

    if not local:
        assert len(PolicyValidator("live", local=False).parse(body).rules) == 3
        return

    with pytest.raises(PolicyValidationError, match="Too many items match"):
        PolicyValidator("example.json", local=local).parse(body)


def test_live_metadata_cannot_hide_duplicate_actor_identities() -> None:
    body = base_policy(local=False)
    body["rules"][1]["parameters"]["allowed_actors"] = [
        {"id": 123, "type": "User", "login": "old-name"},
        {"id": 123, "type": "User", "login": "new-name"},
    ]

    parsed = PolicyValidator("live", local=False).parse(body)
    assert parsed.allowed_actors == (ActorSelector(123, "User"),) * 2
    assert parsed.to_json() != PolicyValidator("local").parse(base_policy()).to_json()


@pytest.mark.parametrize("suffix", ["\n", "\r", "\t"])
def test_rejects_control_whitespace_in_local_paths_and_names(suffix: str) -> None:
    body = base_policy()
    body["name"] += suffix
    body["conditions"]["workflow_path"]["include"][0] += suffix

    with pytest.raises(PolicyValidationError) as error:
        PolicyValidator("example.json").parse(body)

    assert any("example.json:name:" in problem for problem in error.value.errors)
    assert any("include[0]:" in problem for problem in error.value.errors)


def test_schema_errors_preserve_original_array_indices() -> None:
    body = base_policy()
    body["rules"][0]["parameters"]["allowed_events"] = [None, "unknown-event"]

    with pytest.raises(PolicyValidationError) as error:
        PolicyValidator("example.json").parse(body)

    assert any("allowed_events[0]:" in problem for problem in error.value.errors)
    assert any("allowed_events[1]:" in problem for problem in error.value.errors)


def test_workflow_existence_is_still_checked(tmp_path: Path) -> None:
    with pytest.raises(PolicyValidationError, match="does not exist in this checkout"):
        PolicyValidator("example.json", tmp_path).parse(base_policy())


def test_policy_names_must_be_unique(tmp_path: Path) -> None:
    first = base_policy()
    second = copy.deepcopy(first)
    second["rules"][0]["parameters"]["allowed_events"] = ["push"]
    policy_dir, repo_root = write_fixture(tmp_path, [first, second])

    errors = validation_errors(policy_dir, repo_root)

    assert any("name: duplicates policy-0.json" in error for error in errors)


@pytest.mark.parametrize(
    ("include", "exclude"),
    [
        (["~ALL", WORKFLOW], []),
        ([".github/workflows/*.yaml"], []),
        (["~ALL"], ["~ALL"]),
        (["~ALL"], [".github/workflows/*.yaml"]),
    ],
)
def test_rejects_unsupported_workflow_patterns(include, exclude) -> None:
    body = base_policy()
    body["conditions"]["workflow_path"] = {"include": include, "exclude": exclude}
    with pytest.raises(PolicyValidationError):
        PolicyValidator("local").parse(body)


@pytest.mark.parametrize(
    ("catch_all_state", "exception_state", "valid"),
    [
        ("disabled", "disabled", True),
        ("disabled", "active", True),
        ("active", "active", True),
        ("active", "disabled", False),
    ],
)
def test_catch_all_exclusions_need_protection(
    tmp_path, catch_all_state, exception_state, valid
) -> None:
    catch_all = base_policy()
    catch_all["name"] = "NBS: default"
    catch_all["enforcement"] = catch_all_state
    catch_all["conditions"]["workflow_path"] = {
        "include": ["~ALL"],
        "exclude": [WORKFLOW],
    }
    exception = base_policy()
    exception["enforcement"] = exception_state
    policy_dir, repo_root = write_fixture(tmp_path, [catch_all, exception])

    if valid:
        assert len(load_policies(policy_dir, repo_root)) == 2
    else:
        assert any(
            "covered by an active policy" in error
            for error in validation_errors(policy_dir, repo_root)
        )


def test_rejects_unprotected_catch_all_exclusion(tmp_path) -> None:
    catch_all = base_policy()
    catch_all["conditions"]["workflow_path"] = {
        "include": ["~ALL"],
        "exclude": [WORKFLOW],
    }
    policy_dir, repo_root = write_fixture(tmp_path, [catch_all])
    assert any(
        "covered by another policy" in error
        for error in validation_errors(policy_dir, repo_root)
    )


def test_catch_all_excluded_workflow_must_exist(tmp_path) -> None:
    body = base_policy()
    body["conditions"]["workflow_path"] = {"include": ["~ALL"], "exclude": [WORKFLOW]}
    with pytest.raises(PolicyValidationError, match=r"exclude\[0\].*does not exist"):
        PolicyValidator("local", tmp_path).parse(body)


@pytest.mark.parametrize("kind", ["allowed_events", "allowed_actors"])
def test_empty_live_allowlist_is_preserved_for_drift(kind) -> None:
    body = base_policy(local=False)
    body["rules"][0 if kind == "allowed_events" else 1]["parameters"][kind] = []
    parsed = PolicyValidator("live", local=False).parse(body)
    assert parsed.to_json() != PolicyValidator("local").parse(base_policy()).to_json()
    assert any(rule["parameters"].get(kind) == [] for rule in parsed.to_json()["rules"])
