"""Shared argument and error presentation for the policy commands."""

import argparse
import os
import sys
from pathlib import Path

from scripts.policy.model import default_policy_dir, default_repo_root
from scripts.policy.types import PolicyValidationError


def policy_parser(
    description: str, *, repository: bool = False
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--policy-dir",
        type=Path,
        default=default_policy_dir(),
        help="directory containing policy JSON files",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=default_repo_root(),
        help="repository root used to resolve workflow paths",
    )
    if repository:
        parser.add_argument(
            "--repository",
            default=os.environ.get("GITHUB_REPOSITORY"),
            help="repository in OWNER/REPO form (default: GITHUB_REPOSITORY)",
        )
    return parser


def print_validation_errors(error: PolicyValidationError) -> None:
    for problem in error.errors:
        print(f"FAIL {problem}", file=sys.stderr)
