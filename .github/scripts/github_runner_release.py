import argparse
import os

from .helpers import (
    GITHUB_RUNNER_LATEST_VERSION,
    github_output,
    resolve_github_runner_release,
    setup_logger,
)

logger = setup_logger()


def main():
    parser = argparse.ArgumentParser(
        description="Resolve GitHub Actions runner release metadata."
    )
    parser.add_argument(
        "--version",
        default=os.getenv("RUNNER_VERSION") or GITHUB_RUNNER_LATEST_VERSION,
        help="GitHub runner version, or 'latest'. Empty value resolves latest.",
    )
    parser.add_argument(
        "--github-token",
        default=os.getenv("GITHUB_TOKEN"),
        help="GitHub token used to query releases.",
    )
    args = parser.parse_args()

    release = resolve_github_runner_release(args.version, args.github_token)
    logger.info(
        "Resolved GitHub runner release version=%s sha256_by_arch=%s",
        release.version,
        release.sha256_by_arch,
    )

    github_output(logger, "RUNNER_VERSION", release.version)
    github_output(logger, "RUNNER_SHA256_X64", release.sha256_by_arch["x64"])
    github_output(logger, "RUNNER_SHA256_ARM64", release.sha256_by_arch["arm64"])


if __name__ == "__main__":
    main()
