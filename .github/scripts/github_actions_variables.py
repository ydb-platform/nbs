import argparse
import os

from github import Github

from .helpers import fetch_repo_variable, setup_logger

logger = setup_logger()


def update_variable(
    github_token: str,
    github_repository: str,
    variable_name: str,
    value: str,
) -> None:
    github = Github(github_token)
    _, variable = fetch_repo_variable(github, github_repository, variable_name)
    old_value = variable.value

    if old_value == value:
        logger.info("%s is already %s", variable_name, value)
        return

    logger.info("Updating %s: %s -> %s", variable_name, old_value, value)
    variable.edit(value=value)


def main():
    parser = argparse.ArgumentParser(description="Update a GitHub Actions variable.")
    parser.add_argument(
        "--github-token",
        default=os.getenv("GITHUB_TOKEN"),
        help="GitHub token with repository variable write permission.",
    )
    parser.add_argument(
        "--github-repo",
        default=os.getenv("GITHUB_REPOSITORY"),
        help='GitHub repository in the format "owner/repo".',
    )
    parser.add_argument(
        "--variable-name",
        required=True,
        help="GitHub Actions variable name.",
    )
    parser.add_argument(
        "--value",
        required=True,
        help="New variable value.",
    )
    args = parser.parse_args()

    update_variable(
        github_token=args.github_token,
        github_repository=args.github_repo,
        variable_name=args.variable_name,
        value=args.value,
    )


if __name__ == "__main__":
    main()
