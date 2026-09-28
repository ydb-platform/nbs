"""Typed policy models, read-only source contracts, and errors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, Iterable, Protocol

MANAGED_NAME_PREFIX = "NBS: "
REPOSITORY_ROLE_NAMES = {2: "maintain", 4: "write", 5: "admin"}
UNSUPPORTED_SIMULATION_ACTORS = frozenset(
    {"App", "IntegrationInstallation", "BusinessTeam", "EnterpriseTeam"}
)


class PolicyValidationError(ValueError):
    def __init__(self, errors: Iterable[str]):
        self.errors = tuple(errors)
        super().__init__("\n".join(self.errors))


class PolicyApiError(RuntimeError):
    pass


class PolicyCheckError(RuntimeError):
    pass


class PolicySimulationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Actor:
    id: int
    login: str
    type: str


@dataclass(frozen=True)
class ObservedRun:
    id: int
    workflow_path: str
    event: str
    actor: Actor
    conclusion: str | None


@dataclass(frozen=True)
class CheckResult:
    ok: bool
    lines: tuple[str, ...]


@dataclass(frozen=True)
class ActorEvidence:
    team_members: dict[int, frozenset[int]]
    repository_roles: dict[int, str]


@dataclass(frozen=True)
class ActorSelector:
    id: int
    type: str

    def matches(self, actor: Actor, evidence: ActorEvidence) -> bool:
        if self.type == "Team":
            return actor.id in evidence.team_members.get(self.id, ())
        if self.type == "RepositoryRole":
            role_name = REPOSITORY_ROLE_NAMES.get(self.id)
            if role_name is None:
                raise PolicySimulationError(f"unknown RepositoryRole IDs: {self.id}")
            return evidence.repository_roles.get(actor.id) == role_name
        if self.type in UNSUPPORTED_SIMULATION_ACTORS:
            raise PolicySimulationError(
                f"simulation does not support actor types: {self.type}"
            )
        return actor.type == self.type and actor.id == self.id

    def to_json(self) -> dict[str, Any]:
        return {"id": self.id, "type": self.type}


@dataclass(frozen=True)
class ActorRule:
    type: ClassVar[str] = "restrict_actions_actors"
    allowed_actors: tuple[ActorSelector, ...]

    def violation(self, run: ObservedRun, evidence: ActorEvidence) -> str | None:
        if not any(actor.matches(run.actor, evidence) for actor in self.allowed_actors):
            return "actor is not allowed"
        return None

    def to_json(self) -> dict[str, Any]:
        actors = sorted(self.allowed_actors, key=lambda actor: (actor.type, actor.id))
        return {
            "type": self.type,
            "parameters": {"allowed_actors": [actor.to_json() for actor in actors]},
        }


@dataclass(frozen=True)
class EventRule:
    type: ClassVar[str] = "restrict_action_events"
    allowed_events: tuple[str, ...]

    def violation(
        self, run: ObservedRun, evidence: ActorEvidence  # noqa: U100
    ) -> str | None:
        if run.event not in self.allowed_events:
            return f"event {run.event!r} is not allowed"
        return None

    def to_json(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "parameters": {"allowed_events": sorted(self.allowed_events)},
        }


PolicyRule = ActorRule | EventRule


@dataclass(frozen=True)
class Policy:
    name: str
    enforcement: str
    workflow_paths: frozenset[str]
    excluded_workflow_paths: frozenset[str]
    rules: tuple[PolicyRule, ...]

    @property
    def targets_all_workflows(self) -> bool:
        return "~ALL" in self.workflow_paths

    def matches_workflow(self, path: str) -> bool:
        """Match local exact-path or catch-all scopes, applying exclusions first."""
        return path not in self.excluded_workflow_paths and (
            self.targets_all_workflows or path in self.workflow_paths
        )

    @property
    def allowed_actors(self) -> tuple[ActorSelector, ...]:
        return tuple(
            actor
            for rule in self.rules
            if isinstance(rule, ActorRule)
            for actor in rule.allowed_actors
        )

    def violations(self, run: ObservedRun, evidence: ActorEvidence) -> tuple[str, ...]:
        return tuple(
            violation
            for rule in self.rules
            if (violation := rule.violation(run, evidence)) is not None
        )

    def to_json(self) -> dict[str, Any]:
        """Return canonical API fields, ignoring rule/allowlist ordering."""
        return {
            "name": self.name,
            "enforcement": self.enforcement,
            "conditions": {
                "workflow_path": {
                    "include": sorted(self.workflow_paths),
                    "exclude": sorted(self.excluded_workflow_paths),
                }
            },
            "rules": [
                rule.to_json() for rule in sorted(self.rules, key=lambda r: r.type)
            ],
        }


@dataclass(frozen=True)
class PolicyFile:
    path: Path
    policy: Policy

    @property
    def name(self) -> str:
        return self.policy.name

    @property
    def workflow_paths(self) -> frozenset[str]:
        return self.policy.workflow_paths


@dataclass(frozen=True)
class PolicySummary:
    id: int
    name: str


@dataclass(frozen=True, order=True)
class Outcome:
    decision: str
    workflow_path: str
    event: str
    actor_login: str
    actor_type: str
    actor_id: int
    conclusion: str
    violations: tuple[str, ...]


@dataclass(frozen=True)
class SimulationResult:
    ok: bool
    lines: tuple[str, ...]
    allowed: int
    denied: int
    denied_skipped: int


class PolicySource(Protocol):
    """Policy access implemented structurally by GitHubApi and test sources."""

    def list_policies(
        self, repository: str  # noqa: U100
    ) -> tuple[PolicySummary, ...]: ...

    def get_policy(
        self, repository: str, summary: PolicySummary  # noqa: U100
    ) -> Policy: ...


class HistorySource(Protocol):
    """Run history and membership access required by the simulator."""

    def workflow_runs(
        self,
        repository: str,  # noqa: U100
        workflow_path: str | None,  # noqa: U100
        since: datetime,  # noqa: U100
        until: datetime,  # noqa: U100
    ) -> tuple[ObservedRun, ...]:
        """Read one workflow, or all repository runs when workflow_path is None."""
        ...

    def team_member_ids(
        self, repository: str, team_id: int  # noqa: U100
    ) -> frozenset[int]: ...

    def repository_roles(self, repository: str) -> dict[int, str]: ...  # noqa: U100
