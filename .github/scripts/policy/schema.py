"""Load checked-in JSON schemas; all references resolve offline."""

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, validators
from jsonschema.protocols import Validator
from referencing import Registry

SCHEMA_DIR = Path(__file__).with_name("schemas")

# JSON Schema considers 1.0 an integer; retain the tooling's strict JSON-ID check.
_StrictIntegerValidator = validators.extend(
    Draft202012Validator,
    type_checker=Draft202012Validator.TYPE_CHECKER.redefine(
        "integer", lambda _checker, value: type(value) is int  # noqa: U101
    ),
)


@lru_cache(maxsize=1)
def _schemas() -> dict[str, Any]:
    return {
        path.name: json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(SCHEMA_DIR.glob("*.schema.json"))
    }


@lru_cache(maxsize=1)
def _registry() -> Registry:
    # Registry's default retrieval raises for unknown resources; no HTTP fallback.
    return Registry().with_contents(
        (schema["$id"], schema) for schema in _schemas().values()
    )


def policy_schema(*, local: bool = True) -> dict[str, Any]:
    filename = "local-policy.schema.json" if local else "live-policy.schema.json"
    return _schemas()[filename]


@lru_cache(maxsize=2)
def policy_validator(*, local: bool = True) -> Validator:
    return _StrictIntegerValidator(policy_schema(local=local), registry=_registry())
