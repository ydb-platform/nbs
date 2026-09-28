from __future__ import annotations

import os
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from github.GithubException import GithubException
from github.Repository import Repository
from github.Workflow import Workflow as GithubWorkflow
from github.WorkflowRun import WorkflowRun as GithubWorkflowRun

from scripts.helpers import github_client
from scripts.policy.model import API_VERSION, PolicyValidator
from scripts.policy.types import (
    Actor,
    ObservedRun,
    Policy,
    PolicyApiError,
    PolicySummary,
    PolicyValidationError,
)

UTC = timezone.utc
RUN_SEARCH_LIMIT = 1000


def repository_parts(repository: str) -> tuple[str, str]:
    parts = repository.split("/")
    if len(parts) != 2 or not all(parts):
        raise PolicyApiError("repository must have OWNER/REPO form")
    return parts[0], parts[1]


def format_time(value: datetime) -> str:
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def _parse_run(value: GithubWorkflowRun, workflow_path: str | None) -> ObservedRun:
    try:
        run_id = value.id
        event = value.event
        path = value.path
        actor = value.actor
        conclusion = value.conclusion
        actor_id = actor.id
        actor_login = actor.login
        actor_type = actor.type
    except (AttributeError, TypeError) as error:
        raise PolicyApiError("workflow run has an undocumented shape") from error
    if (
        type(run_id) is not int
        or run_id <= 0
        or not isinstance(event, str)
        or not event
        or not isinstance(path, str)
        or not path
        or (conclusion is not None and not isinstance(conclusion, str))
    ):
        raise PolicyApiError("workflow run has an undocumented shape")
    normalized_path = path.split("@", 1)[0]
    if workflow_path is not None and normalized_path != workflow_path:
        raise PolicyApiError(
            f"run {run_id} reports path {path!r}, expected {workflow_path!r}"
        )
    if (
        type(actor_id) is not int
        or actor_id <= 0
        or not isinstance(actor_login, str)
        or not actor_login
        or not isinstance(actor_type, str)
        or not actor_type
    ):
        raise PolicyApiError(f"run {run_id} has an invalid actor")
    return ObservedRun(
        id=run_id,
        workflow_path=normalized_path,
        event=event,
        actor=Actor(id=actor_id, login=actor_login, type=actor_type),
        conclusion=conclusion,
    )


def _read_workflow_window(
    workflow: GithubWorkflow | Repository,
    workflow_path: str | None,
    since: datetime,
    until: datetime,
) -> tuple[ObservedRun, ...]:
    created = f"{format_time(since)}..{format_time(until)}"
    paginated = (
        workflow.get_workflow_runs(created=created)
        if workflow_path is None
        else workflow.get_runs(created=created)
    )
    total = paginated.totalCount
    if type(total) is not int or total < 0:
        raise PolicyApiError("workflow run list has an undocumented shape")
    if total >= RUN_SEARCH_LIMIT:
        if since >= until:
            raise PolicyApiError(
                f"{workflow_path} has at least {RUN_SEARCH_LIMIT} runs " "in one second"
            )
        seconds = (until - since) // timedelta(seconds=1)
        midpoint = since + timedelta(seconds=seconds // 2)
        right_since = midpoint + timedelta(seconds=1)
        left = _read_workflow_window(workflow, workflow_path, since, midpoint)
        right = _read_workflow_window(workflow, workflow_path, right_since, until)
        runs = left + right
    else:
        raw_runs = tuple(paginated)
        if len(raw_runs) != total:
            raise PolicyApiError(
                f"read {len(raw_runs)} of {total} runs for {workflow_path}"
            )
        runs = tuple(_parse_run(run, workflow_path) for run in raw_runs)
    ids = [run.id for run in runs]
    if len(ids) != len(set(ids)):
        raise PolicyApiError("workflow history contains duplicate run IDs")
    return runs


class GitHubApi:
    """Shared PyGithub access for policy checks and historical simulation."""

    def __init__(self, token: str) -> None:
        if not token.strip():
            raise PolicyApiError("GITHUB_TOKEN is required")
        self._client = github_client(token)
        self._repositories: dict[str, Repository] = {}
        self._workflows: dict[tuple[str, str], GithubWorkflow] = {}

    @classmethod
    def from_env(cls) -> GitHubApi:
        token = os.environ.get("GITHUB_TOKEN") or ""
        return cls(token)

    def _get(self, path: str) -> Any:
        try:
            _, data = self._client.requester.requestJsonAndCheck(
                "GET",
                path,
                headers={
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": API_VERSION,
                },
            )
            return data
        except GithubException as error:
            raise PolicyApiError(
                f"GitHub API returned HTTP {error.status} for {path}: {error.data}"
            ) from error

    @staticmethod
    def _policy_path(repository: str) -> str:
        owner, repo = repository_parts(repository)
        return "/repos/{}/{}/actions/policies".format(
            urllib.parse.quote(owner, safe=""), urllib.parse.quote(repo, safe="")
        )

    def list_policies(self, repository: str) -> tuple[PolicySummary, ...]:
        base = self._policy_path(repository)
        policies: list[PolicySummary] = []
        expected_total: int | None = None
        seen_ids: set[int] = set()
        for page in range(1, 51):
            response = self._get(f"{base}?per_page=100&page={page}&has_parents=false")
            if not isinstance(response, dict):
                raise PolicyApiError("policy list is not an object")
            total = response.get("total_count")
            page_policies = response.get("policies")
            if (
                type(total) is not int
                or total < 0
                or not isinstance(page_policies, list)
            ):
                raise PolicyApiError("policy list has an undocumented shape")
            if expected_total is None:
                expected_total = total
            elif total != expected_total:
                raise PolicyApiError("policy count changed during pagination")
            for policy in page_policies:
                if (
                    not isinstance(policy, dict)
                    or type(policy.get("id")) is not int
                    or policy["id"] <= 0
                    or not isinstance(policy.get("name"), str)
                ):
                    raise PolicyApiError("policy list entry has an undocumented shape")
                if policy["id"] in seen_ids:
                    raise PolicyApiError("policy list contains duplicate IDs")
                seen_ids.add(policy["id"])
                policies.append(PolicySummary(id=policy["id"], name=policy["name"]))
            if len(policies) >= total or len(page_policies) < 100:
                break
        if expected_total is None or len(policies) != expected_total:
            raise PolicyApiError(
                f"read {len(policies)} of {expected_total or 0} repository policies"
            )
        return tuple(policies)

    def get_policy(self, repository: str, summary: PolicySummary) -> Policy:
        body = self._get(f"{self._policy_path(repository)}/{summary.id}")
        if (
            not isinstance(body, dict)
            or type(body.get("id")) is not int
            or body["id"] != summary.id
            or body.get("name") != summary.name
        ):
            raise PolicyApiError(
                f"full read for policy {summary.id} has an undocumented shape"
            )
        try:
            return PolicyValidator(f"policy {summary.id}", local=False).parse(body)
        except PolicyValidationError as error:
            raise PolicyApiError(str(error)) from error

    def _repository(self, repository: str) -> Repository:
        cached = self._repositories.get(repository)
        if cached is None:
            cached = self._client.get_repo(repository)
            self._repositories[repository] = cached
        return cached

    def _workflow(self, repository: str, workflow_path: str) -> GithubWorkflow:
        key = (repository, workflow_path)
        cached = self._workflows.get(key)
        if cached is None:
            cached = self._repository(repository).get_workflow(Path(workflow_path).name)
            self._workflows[key] = cached
        return cached

    def workflow_runs(
        self,
        repository: str,
        workflow_path: str | None,
        since: datetime,
        until: datetime,
    ) -> tuple[ObservedRun, ...]:
        try:
            return _read_workflow_window(
                (
                    self._repository(repository)
                    if workflow_path is None
                    else self._workflow(repository, workflow_path)
                ),
                workflow_path,
                since,
                until,
            )
        except GithubException as error:
            raise PolicyApiError(
                f"cannot read runs for {workflow_path}: {error}"
            ) from error

    def team_member_ids(self, repository: str, team_id: int) -> frozenset[int]:
        owner, _ = repository_parts(repository)
        try:
            team = self._client.get_organization(owner).get_team(team_id)
            member_ids = {member.id for member in team.get_members(role="all")}
        except GithubException as error:
            raise PolicyApiError(f"cannot read team {team_id}: {error}") from error
        if any(
            type(member_id) is not int or member_id <= 0 for member_id in member_ids
        ):
            raise PolicyApiError(f"team {team_id} returned an invalid member")
        return frozenset(member_ids)

    def repository_roles(self, repository: str) -> dict[int, str]:
        try:
            collaborators = self._repository(repository).get_collaborators(
                affiliation="all"
            )
            roles = {
                collaborator.id: collaborator.role_name
                for collaborator in collaborators
            }
        except GithubException as error:
            raise PolicyApiError(
                f"cannot read collaborators for {repository}: {error}"
            ) from error
        if any(
            type(actor_id) is not int
            or actor_id <= 0
            or not isinstance(role_name, str)
            or not role_name
            for actor_id, role_name in roles.items()
        ):
            raise PolicyApiError("collaborator list has an undocumented shape")
        return roles
