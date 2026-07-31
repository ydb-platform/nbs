from __future__ import annotations

import math
import urllib.parse
from collections.abc import Iterable, Mapping
from typing import Any

from opentelemetry.proto_json.common.v1.common import AnyValue, ArrayValue, KeyValue

MAX_ATTRIBUTES = 256
MAX_ATTRIBUTE_LENGTH = 8_192


def _clean_scalar(value: Any) -> str | bool | int | float:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if value is None:
        return ""
    text = str(value)
    if len(text) > MAX_ATTRIBUTE_LENGTH:
        return f"{text[:MAX_ATTRIBUTE_LENGTH]}…"
    return text


def clean_attributes(values: Mapping[str, Any] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, raw_value in list((values or {}).items())[:MAX_ATTRIBUTES]:
        key = str(raw_key)[:256]
        if isinstance(raw_value, (list, tuple)):
            result[key] = [_clean_scalar(value) for value in raw_value[:128]]
        else:
            result[key] = _clean_scalar(raw_value)
    return result


class ResourceAttributes(dict[str, Any]):
    """Normalized OTLP resource attributes with common construction helpers."""

    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        super().__init__(clean_attributes(values))

    def with_attributes(
        self,
        values: Mapping[str, Any],
    ) -> ResourceAttributes:
        merged = dict(self)
        merged.update(
            {
                key: value
                for key, value in values.items()
                if value is not None and value != "" and value != []
            }
        )
        return type(self)(merged)

    @classmethod
    def from_github(
        cls,
        *,
        environment: Mapping[str, Any],
        workflow_run: Mapping[str, Any] | None = None,
    ) -> ResourceAttributes:
        server_url = str(
            environment.get("GITHUB_SERVER_URL") or "https://github.com"
        ).rstrip("/")
        if workflow_run is None:
            values: dict[str, Any] = {
                "github.repository": environment.get("GITHUB_REPOSITORY", ""),
                "github.workflow": environment.get("GITHUB_WORKFLOW", ""),
                "github.workflow.ref": environment.get("GITHUB_WORKFLOW_REF", ""),
                "github.job": environment.get("GITHUB_JOB", ""),
                "github.run.id": environment.get("GITHUB_RUN_ID", ""),
                "github.run.number": environment.get("GITHUB_RUN_NUMBER", ""),
                "github.run.attempt": environment.get("GITHUB_RUN_ATTEMPT", ""),
                "github.event.name": environment.get("GITHUB_EVENT_NAME", ""),
                "github.actor": environment.get("GITHUB_ACTOR", ""),
                "github.sha": environment.get("GITHUB_SHA", ""),
                "github.ref": environment.get("GITHUB_REF", ""),
            }
        else:
            repository = workflow_run.get("repository") or {}
            if not isinstance(repository, Mapping):
                repository = {}
            actor = workflow_run.get("actor") or {}
            if not isinstance(actor, Mapping):
                actor = {}
            triggering_actor = workflow_run.get("triggering_actor") or {}
            if not isinstance(triggering_actor, Mapping):
                triggering_actor = {}
            pull_requests = workflow_run.get("pull_requests") or []
            if not isinstance(pull_requests, list):
                pull_requests = []
            values = {
                "github.repository": repository.get("full_name")
                or environment.get("GITHUB_REPOSITORY", ""),
                "github.workflow": workflow_run.get("name", ""),
                "github.workflow.path": workflow_run.get("path", ""),
                "github.run.id": workflow_run.get("id", ""),
                "github.run.number": workflow_run.get("run_number", ""),
                "github.run.attempt": workflow_run.get("run_attempt", 1),
                "github.run.url": workflow_run.get("html_url", ""),
                "github.event.name": workflow_run.get("event", ""),
                "github.actor": actor.get("login", ""),
                "github.triggering_actor": triggering_actor.get("login", ""),
                "github.sha": workflow_run.get("head_sha", ""),
                "github.ref.name": workflow_run.get("head_branch", ""),
                "github.pull_request.number": [
                    item.get("number")
                    for item in pull_requests
                    if isinstance(item, Mapping) and item.get("number") is not None
                ],
                "ci.conclusion": workflow_run.get("conclusion", ""),
                "ci.display_title": workflow_run.get("display_title", ""),
            }

        repository_name = str(values.get("github.repository") or "")
        run_id = values.get("github.run.id")
        sha = str(values.get("github.sha") or "")
        if repository_name and run_id and not values.get("github.run.url"):
            values["github.run.url"] = (
                f"{server_url}/{repository_name}/actions/runs/"
                f"{urllib.parse.quote(str(run_id), safe='')}"
            )
        if repository_name and sha:
            values["github.commit.url"] = (
                f"{server_url}/{repository_name}/commit/"
                f"{urllib.parse.quote(sha, safe='')}"
            )

        return cls(
            {
                key: value
                for key, value in values.items()
                if value is not None and value != "" and value != []
            }
        )


def any_value(value: Any) -> AnyValue:
    if isinstance(value, bool):
        return AnyValue(bool_value=value)
    if isinstance(value, int):
        return AnyValue(int_value=value)
    if isinstance(value, float):
        return AnyValue(double_value=value)
    if isinstance(value, (list, tuple)):
        return AnyValue(array_value=ArrayValue(values=[any_value(v) for v in value]))
    return AnyValue(string_value=str(value))


def value_from_any(value: AnyValue) -> Any:
    if value.string_value is not None:
        return value.string_value
    if value.bool_value is not None:
        return value.bool_value
    if value.int_value is not None:
        return value.int_value
    if value.double_value is not None:
        return value.double_value
    if value.array_value is not None:
        return [value_from_any(item) for item in value.array_value.values]
    if value.kvlist_value is not None:
        return decode_attributes(value.kvlist_value.values)
    if value.bytes_value is not None:
        return value.bytes_value
    return ""


def encode_attributes(values: Mapping[str, Any]) -> list[KeyValue]:
    return [
        KeyValue(key=key, value=any_value(value))
        for key, value in clean_attributes(values).items()
    ]


def decode_attributes(values: Iterable[KeyValue]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in list(values)[:MAX_ATTRIBUTES]:
        key = str(item.key or "")[:256]
        if key and item.value is not None:
            result[key] = value_from_any(item.value)
    return clean_attributes(result)
