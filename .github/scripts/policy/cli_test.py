from pathlib import Path

import pytest

from scripts.policy import check, simulate, validate
from scripts.policy.cli import print_validation_errors
from scripts.policy.types import PolicyValidationError


@pytest.mark.parametrize("command", [check, simulate, validate])
def test_shared_policy_arguments(command, monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(
        "sys.argv",
        [command.__name__, "--policy-dir", "policies", "--repo-root", "repo"],
    )

    args = command.parse_args()

    assert args.policy_dir == Path("policies")
    assert args.repo_root == Path("repo")
    if command is not validate:
        assert args.repository == "owner/repo"


@pytest.mark.parametrize("command", [check, simulate])
def test_repository_argument_overrides_environment(command, monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr("sys.argv", [command.__name__, "--repository", "another/repo"])

    assert command.parse_args().repository == "another/repo"


def test_validation_errors_are_printed_to_stderr(capsys) -> None:
    print_validation_errors(PolicyValidationError(["first", "second"]))

    captured = capsys.readouterr()
    assert not captured.out
    assert captured.err == "FAIL first\nFAIL second\n"
