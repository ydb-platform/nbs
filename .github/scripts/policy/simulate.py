from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from scripts.policy.api import GitHubApi, format_time
from scripts.policy.cli import policy_parser, print_validation_errors
from scripts.policy.model import load_policies
from scripts.policy.types import (
    REPOSITORY_ROLE_NAMES,
    UNSUPPORTED_SIMULATION_ACTORS,
    ActorEvidence,
    HistorySource,
    Outcome,
    PolicyApiError,
    PolicyFile,
    PolicySimulationError,
    PolicyValidationError,
    SimulationResult,
)

UTC = timezone.utc


def _parse_time(
    value: str, name: str, *, relative_to: datetime | None = None
) -> datetime:
    try:
        if relative_to is not None and re.fullmatch(r"-[0-9]+h", value):
            parsed = relative_to - timedelta(hours=int(value[1:-1]))
        else:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, OverflowError) as error:
        expected = "an ISO-8601 timestamp with a timezone"
        if relative_to is not None:
            expected += " or negative whole hours (e.g. --since=-24h)"
        raise PolicySimulationError(f"{name} must be {expected}") from error
    if parsed.tzinfo is None:
        raise PolicySimulationError(f"{name} must include a timezone")
    return parsed.astimezone(UTC).replace(microsecond=0)


def _required_actors(
    policies: tuple[PolicyFile, ...],
) -> tuple[set[int], set[int]]:
    actors = tuple(
        actor for policy in policies for actor in policy.policy.allowed_actors
    )
    team_ids = {actor.id for actor in actors if actor.type == "Team"}
    role_ids = {actor.id for actor in actors if actor.type == "RepositoryRole"}
    unsupported_types = {
        actor.type for actor in actors if actor.type in UNSUPPORTED_SIMULATION_ACTORS
    }
    if unsupported_types:
        raise PolicySimulationError(
            "simulation does not support actor types: "
            + ", ".join(sorted(unsupported_types))
        )
    unknown_roles = role_ids - REPOSITORY_ROLE_NAMES.keys()
    if unknown_roles:
        raise PolicySimulationError(
            "unknown RepositoryRole IDs: "
            + ", ".join(str(value) for value in sorted(unknown_roles))
        )
    return team_ids, role_ids


def _load_actor_evidence(
    history: HistorySource,
    repository: str,
    policies: tuple[PolicyFile, ...],
) -> ActorEvidence:
    team_ids, role_ids = _required_actors(policies)
    team_members = {
        team_id: history.team_member_ids(repository, team_id)
        for team_id in sorted(team_ids)
    }
    repository_roles = history.repository_roles(repository) if role_ids else {}
    return ActorEvidence(
        team_members=team_members,
        repository_roles=repository_roles,
    )


def simulate_repository_policies(
    history: HistorySource,
    repository: str,
    policies: tuple[PolicyFile, ...],
    since: datetime,
    until: datetime,
    *,
    show_allowed: bool = False,
) -> SimulationResult:
    since = since.astimezone(UTC).replace(microsecond=0)
    until = until.astimezone(UTC).replace(microsecond=0)
    if since > until:
        raise PolicySimulationError("--since must not be later than --until")

    evidence = _load_actor_evidence(history, repository, policies)
    workflow_paths = {
        path for policy in policies for path in policy.workflow_paths if path != "~ALL"
    }
    if any(policy.policy.targets_all_workflows for policy in policies):
        # Repository history also includes new, renamed, and since-deleted workflows.
        all_runs = history.workflow_runs(repository, None, since, until)
        workflow_paths.update(run.workflow_path for run in all_runs)
        grouped_runs = {path: [] for path in sorted(workflow_paths)}
        for run in all_runs:
            grouped_runs[run.workflow_path].append(run)
        runs_by_path = {path: tuple(runs) for path, runs in grouped_runs.items()}
    else:
        runs_by_path = {
            path: history.workflow_runs(repository, path, since, until)
            for path in sorted(workflow_paths)
        }
    policies_by_path = {
        path: tuple(
            policy.policy for policy in policies if policy.policy.matches_workflow(path)
        )
        for path in runs_by_path
    }
    runs_by_path = {
        path: runs for path, runs in runs_by_path.items() if policies_by_path[path]
    }

    outcomes: dict[Outcome, list[int]] = defaultdict(list)
    allowed = 0
    denied = 0
    denied_skipped = 0
    for workflow_path, runs in runs_by_path.items():
        for run in runs:
            violations = tuple(
                f"{policy.name}: {violation}"
                for policy in policies_by_path[workflow_path]
                for violation in policy.violations(run, evidence)
            )
            outcome = Outcome(
                decision="DENY" if violations else "ALLOW",
                workflow_path=workflow_path,
                event=run.event,
                actor_login=run.actor.login,
                actor_type=run.actor.type,
                actor_id=run.actor.id,
                conclusion=run.conclusion or "not_completed",
                violations=violations,
            )
            outcomes[outcome].append(run.id)
            if violations:
                denied += 1
                if run.conclusion == "skipped":
                    denied_skipped += 1
            else:
                allowed += 1

    lines = [
        f"WINDOW  {format_time(since)}..{format_time(until)}",
        "NOTE    disabled policies are evaluated hypothetically; "
        "team and role membership is current",
    ]
    for workflow_path, runs in runs_by_path.items():
        if not runs:
            lines.append(f"NO-RUNS {workflow_path}")
    for outcome, run_ids in sorted(outcomes.items()):
        if outcome.decision == "ALLOW" and not show_allowed:
            continue
        line = (
            f"{outcome.decision:<7} count={len(run_ids)} "
            f"workflow={outcome.workflow_path} event={outcome.event} "
            f"actor={outcome.actor_login} "
            f"({outcome.actor_type}:{outcome.actor_id}) "
            f"observed_conclusion={outcome.conclusion}"
        )
        if outcome.violations:
            line += " reason=" + "; ".join(outcome.violations)
        lines.append(line)
        lines.extend(
            f"          run={run_id} url=https://github.com/{repository}/actions/runs/{run_id}"
            for run_id in sorted(run_ids)
        )
    lines.append(
        f"SUMMARY allowed={allowed} denied={denied} "
        f"denied_skipped={denied_skipped} "
        f"no_runs={sum(not runs for runs in runs_by_path.values())}"
    )
    return SimulationResult(
        ok=denied == 0,
        lines=tuple(lines),
        allowed=allowed,
        denied=denied,
        denied_skipped=denied_skipped,
    )


def parse_args() -> argparse.Namespace:
    parser = policy_parser(
        "Estimate how reviewed policies affect historical workflow runs.",
        repository=True,
    )
    parser.add_argument(
        "--since",
        help="ISO-8601 timestamp (e.g. 2026-09-28T00:00:00Z) or negative whole "
        "hours before --until (e.g. --since=-24h; default: -24h)",
    )
    parser.add_argument(
        "--until",
        help="ISO-8601 timestamp (e.g. 2026-09-29T00:00:00Z; default: current time)",
    )
    parser.add_argument(
        "--show-allowed",
        action="store_true",
        help="print allowed actor/event groups as well as denials",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.repository:
        print(
            "FAIL --repository or GITHUB_REPOSITORY is required",
            file=sys.stderr,
        )
        return 2
    try:
        until = (
            _parse_time(args.until, "--until")
            if args.until
            else datetime.now(UTC).replace(microsecond=0)
        )
        since = _parse_time(args.since or "-24h", "--since", relative_to=until)
        policies = load_policies(args.policy_dir, args.repo_root)
        history = GitHubApi.from_env()
        result = simulate_repository_policies(
            history,
            args.repository,
            policies,
            since,
            until,
            show_allowed=args.show_allowed,
        )
    except PolicyValidationError as error:
        print_validation_errors(error)
        return 1
    except (PolicyApiError, PolicySimulationError) as error:
        print(f"UNKNOWN {error}", file=sys.stderr)
        return 2
    for line in result.lines:
        print(line)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
