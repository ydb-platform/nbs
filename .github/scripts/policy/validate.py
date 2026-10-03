from __future__ import annotations

import argparse

from scripts.policy.cli import policy_parser, print_validation_errors
from scripts.policy.model import load_policies
from scripts.policy.types import PolicyValidationError


def parse_args() -> argparse.Namespace:
    return policy_parser(
        "Validate repository-scoped GitHub Actions execution policies."
    ).parse_args()


def main() -> int:
    args = parse_args()
    try:
        policies = load_policies(args.policy_dir, args.repo_root)
    except PolicyValidationError as error:
        print_validation_errors(error)
        return 1
    for policy in policies:
        print(f"OK   {policy.path.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
