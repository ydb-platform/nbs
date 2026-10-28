from __future__ import annotations

import argparse
import json
import sys
from collections import Counter

from scripts.policy.api import GitHubApi
from scripts.policy.cli import policy_parser, print_validation_errors
from scripts.policy.model import load_policies
from scripts.policy.types import (
    MANAGED_NAME_PREFIX,
    CheckResult,
    PolicyApiError,
    PolicyCheckError,
    PolicyFile,
    PolicySource,
    PolicyValidationError,
)


def check_repository_policies(
    api: PolicySource, repository: str, desired: tuple[PolicyFile, ...]
) -> CheckResult:
    summaries = api.list_policies(repository)
    desired_by_name = {policy.name: policy for policy in desired}
    relevant = [
        policy
        for policy in summaries
        if policy.name in desired_by_name or policy.name.startswith(MANAGED_NAME_PREFIX)
    ]
    counts = Counter(policy.name for policy in relevant)
    duplicates = sorted(name for name, count in counts.items() if count > 1)
    if duplicates:
        raise PolicyCheckError(
            "duplicate live managed policy names: " + ", ".join(duplicates)
        )

    live_by_name = {
        summary.name: api.get_policy(repository, summary)
        for summary in relevant
        if summary.name in desired_by_name
    }
    lines: list[str] = []
    ok = True
    for policy in desired:
        live = live_by_name.get(policy.name)
        if live is None:
            ok = False
            lines.append(f"DRIFT    {policy.path.name}: live policy is missing")
            continue
        expected, actual = policy.policy.to_json(), live.to_json()
        if actual != expected:
            ok = False
            lines.append(f"DRIFT    {policy.path.name}: live policy differs")
            lines.append("          want: " + json.dumps(expected, sort_keys=True))
            lines.append("          live: " + json.dumps(actual, sort_keys=True))
        else:
            lines.append(f"IN-SYNC  {policy.path.name}")

    for summary in sorted(relevant, key=lambda policy: policy.name):
        if (
            summary.name.startswith(MANAGED_NAME_PREFIX)
            and summary.name not in desired_by_name
        ):
            ok = False
            lines.append(
                f"STALE    {summary.name} (id {summary.id}): no policy file owns it"
            )
    for summary in summaries:
        if not summary.name.startswith(MANAGED_NAME_PREFIX):
            lines.append(f"UNMANAGED {summary.name} (id {summary.id}): ignored")
    return CheckResult(ok=ok, lines=tuple(lines))


def parse_args() -> argparse.Namespace:
    return policy_parser(
        "Compare reviewed policies with live repository Actions policies.",
        repository=True,
    ).parse_args()


def main() -> int:
    args = parse_args()
    if not args.repository:
        print("FAIL --repository or GITHUB_REPOSITORY is required", file=sys.stderr)
        return 2
    try:
        policies = load_policies(args.policy_dir, args.repo_root)
        result = check_repository_policies(
            GitHubApi.from_env(), args.repository, policies
        )
    except PolicyValidationError as error:
        print_validation_errors(error)
        return 1
    except (PolicyApiError, PolicyCheckError) as error:
        print(f"UNKNOWN {error}", file=sys.stderr)
        return 2
    for line in result.lines:
        print(line)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
