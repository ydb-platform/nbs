from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.policy.schema import policy_validator
from scripts.policy.types import (
    ActorRule,
    ActorSelector,
    EventRule,
    Policy,
    PolicyFile,
    PolicyRule,
    PolicyValidationError,
)

API_VERSION = "2026-03-10"


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_policy_dir() -> Path:
    return default_repo_root() / ".github" / "policy"


@dataclass
class PolicyValidator:
    """Validate the schema, then check repository facts and build typed rules."""

    filename: str
    repo_root: Path | None = None
    local: bool = True
    errors: list[str] = field(default_factory=list, init=False)

    def error(self, location: str, message: str) -> None:
        problem = f"{self.filename}:{location}: {message}"
        if problem not in self.errors:
            self.errors.append(problem)

    def parse(self, value: Any) -> Policy:
        self.errors.clear()
        for error in policy_validator(local=self.local).iter_errors(value):
            self.error(error.json_path.removeprefix("$."), error.message)
        if self.errors:
            raise PolicyValidationError(self.errors)

        # GitHub treats omitted/null conditions and empty includes as all workflows.
        conditions = value.get("conditions") or {}
        workflow_paths = conditions.get(
            "workflow_path", {"include": ["~ALL"], "exclude": []}
        )
        if self.local and self.repo_root is not None:
            for field_name in ("include", "exclude"):
                for index, path in enumerate(workflow_paths[field_name]):
                    if path != "~ALL" and not (self.repo_root / path).is_file():
                        self.error(
                            f"conditions.workflow_path.{field_name}[{index}]",
                            "does not exist in this checkout",
                        )

        rules: list[PolicyRule] = []
        for rule in value.get("rules", []):
            parameters = rule.get("parameters", {})
            if rule["type"] == ActorRule.type:
                actors = tuple(
                    ActorSelector(id=actor["id"], type=actor["type"])
                    for actor in parameters.get("allowed_actors", [])
                )
                rules.append(ActorRule(actors))
            else:
                rules.append(EventRule(tuple(parameters.get("allowed_events", []))))

        if self.errors:
            raise PolicyValidationError(self.errors)
        return Policy(
            name=value["name"],
            enforcement=value["enforcement"],
            workflow_paths=frozenset(workflow_paths["include"] or ["~ALL"]),
            excluded_workflow_paths=frozenset(workflow_paths["exclude"]),
            rules=tuple(rules),
        )


def load_policies(
    policy_dir: Path | None = None, repo_root: Path | None = None
) -> tuple[PolicyFile, ...]:
    policy_dir = (policy_dir or default_policy_dir()).resolve()
    repo_root = (repo_root or default_repo_root()).resolve()
    errors: list[str] = []
    if not policy_dir.is_dir():
        raise PolicyValidationError([f"{policy_dir}: policy directory does not exist"])
    paths = sorted(policy_dir.glob("*.json"))
    if not paths:
        raise PolicyValidationError([f"{policy_dir}: no policy JSON files found"])
    policies: list[PolicyFile] = []
    for path in paths:
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            errors.append(f"{path.name}:$: invalid JSON: {error}")
            continue
        try:
            policy = PolicyValidator(path.name, repo_root).parse(body)
        except PolicyValidationError as error:
            errors.extend(error.errors)
            continue
        policies.append(PolicyFile(path=path, policy=policy))

    names: dict[str, str] = {}
    for policy in policies:
        previous = names.setdefault(policy.name, policy.path.name)
        if previous != policy.path.name:
            errors.append(f"{policy.path.name}:name: duplicates {previous}")
        if not policy.policy.targets_all_workflows:
            continue
        for excluded in sorted(policy.policy.excluded_workflow_paths):
            protecting = [
                other.policy
                for other in policies
                if other is not policy and other.policy.matches_workflow(excluded)
            ]
            if not protecting:
                errors.append(
                    f"{policy.path.name}:conditions.workflow_path.exclude: "
                    f"{excluded} must be covered by another policy"
                )
            elif policy.policy.enforcement == "active" and not any(
                other.enforcement == "active" for other in protecting
            ):
                errors.append(
                    f"{policy.path.name}:conditions.workflow_path.exclude: "
                    f"{excluded} must be covered by an active policy before activating the catch-all"
                )

    if errors:
        raise PolicyValidationError(errors)
    return tuple(policies)
