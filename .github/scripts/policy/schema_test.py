import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from referencing.exceptions import Unresolvable

from scripts.policy.model import API_VERSION, PolicyValidator
from scripts.policy.schema import SCHEMA_DIR, _registry, policy_schema, policy_validator
from scripts.policy.types import PolicyValidationError

SCHEMA_PATHS = sorted(SCHEMA_DIR.glob("*.schema.json"))
UPSTREAM_PATH = SCHEMA_DIR / "github-actions-policy.schema.json"
EXAMPLE_PATH = Path(__file__).with_name("testdata") / "github-policy-response.json"


def github_validator() -> Draft202012Validator:
    schema = json.loads(UPSTREAM_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, registry=_registry())


def schema_refs(value):
    if isinstance(value, dict):
        if "$ref" in value:
            yield value["$ref"]
        for child in value.values():
            yield from schema_refs(child)
    elif isinstance(value, list):
        for child in value:
            yield from schema_refs(child)


@pytest.mark.parametrize("path", SCHEMA_PATHS, ids=lambda path: path.name)
def test_checked_in_schema_is_valid_and_references_resolve(path: Path) -> None:
    schema = json.loads(path.read_text(encoding="utf-8"))

    Draft202012Validator.check_schema(schema)
    resolver = _registry().resolver(schema["$id"])
    for ref in schema_refs(schema):
        resolver.lookup(ref)


@pytest.mark.parametrize("local", [True, False])
def test_policy_schema_is_loaded_from_json(local: bool) -> None:
    name = "local-policy.schema.json" if local else "live-policy.schema.json"

    assert policy_schema(local=local) == json.loads(
        (SCHEMA_DIR / name).read_text(encoding="utf-8")
    )


def test_vendor_revision_matches_the_requested_api_version() -> None:
    schema = json.loads(UPSTREAM_PATH.read_text(encoding="utf-8"))
    source = schema["x-source"]

    assert source["api_version"] == API_VERSION
    assert source["path"].endswith(f".{API_VERSION}.json")
    assert source["revision"] == "0029c0d4a6d330559abe0e45b22263bbe1c6b1a7"
    assert "nullable" not in json.dumps(schema["$defs"])


def test_github_published_example_validates_offline(monkeypatch) -> None:
    def no_network(*_args, **_kwargs):  # noqa: U101
        pytest.fail("schema validation attempted a network connection")

    monkeypatch.setattr("socket.create_connection", no_network)
    example = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))

    github_validator().validate(example)
    with pytest.raises(Unresolvable):
        _registry().resolver().lookup("https://example.invalid/missing-schema.json")


def test_github_request_and_nbs_restrictions_are_separate() -> None:
    validator = github_validator()
    request = validator.evolve(schema=validator.schema["$defs"]["request"])
    body = {"name": "Minimal GitHub policy", "enforcement": "evaluate"}

    request.validate(body)
    with pytest.raises(PolicyValidationError):
        PolicyValidator("local.json").parse(body)


@pytest.mark.parametrize("condition", ["omitted", None, {}])
def test_upstream_nullable_and_optional_conditions_are_preserved(condition) -> None:
    response = {
        "id": 1,
        "name": "Minimal GitHub policy",
        "target": "actions",
        "source_type": "Repository",
        "source": "owner/repo",
        "enforcement": "active",
    }
    if condition != "omitted":
        response["conditions"] = condition

    github_validator().validate(response)
    parsed = PolicyValidator("live", local=False).parse(response)
    assert parsed.workflow_paths == frozenset({"~ALL"})
    assert parsed.rules == ()


@pytest.mark.parametrize("field", ["id", "target", "source_type", "source"])
def test_live_schema_requires_github_response_metadata(field: str) -> None:
    response = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
    response["conditions"] = {
        "workflow_path": {"include": [".github/workflows/example.yaml"], "exclude": []}
    }
    validator = policy_validator(local=False)
    validator.validate(response)
    del response[field]

    errors = list(validator.iter_errors(response))

    assert any(error.message == f"'{field}' is a required property" for error in errors)
