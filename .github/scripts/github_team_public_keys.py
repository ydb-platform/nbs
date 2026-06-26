import argparse
import os
import sys

from github import Auth as GithubAuth
from github import Github

try:
    from .helpers import fetch_github_team_public_keys, setup_logger
except ImportError:
    from helpers import fetch_github_team_public_keys, setup_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Print public SSH keys for members of a GitHub team."
    )
    parser.add_argument("--github-token", default=os.environ.get("GITHUB_TOKEN"))
    parser.add_argument("--github-org", default=os.environ.get("ORG"), required=False)
    parser.add_argument(
        "--github-team-slug", default=os.environ.get("TEAM"), required=False
    )
    return parser.parse_args()


def main() -> int:
    setup_logger()
    args = parse_args()

    missing_args = [
        name
        for name, value in (
            ("--github-token", args.github_token),
            ("--github-org", args.github_org),
            ("--github-team-slug", args.github_team_slug),
        )
        if not value
    ]
    if missing_args:
        print(f"Missing required arguments: {', '.join(missing_args)}", file=sys.stderr)
        return 2

    gh = Github(auth=GithubAuth.Token(args.github_token))
    for key in fetch_github_team_public_keys(
        gh, args.github_org, args.github_team_slug
    ):
        print(key)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
