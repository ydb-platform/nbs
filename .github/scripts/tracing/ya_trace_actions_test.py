from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).parents[3]


def _action(name: str) -> dict:
    path = REPOSITORY_ROOT / ".github" / "actions" / name / "action.yaml"
    return yaml.safe_load(path.read_text())


def _step(action: dict, name: str) -> dict:
    return next(step for step in action["runs"]["steps"] if step.get("name") == name)


def test_test_action_omits_retry_argument_from_first_ya_make() -> None:
    script = _step(_action("test"), "ya test")["run"]

    assert "retry_params=()" in script
    assert '"${retry_params[@]}"' in script
    assert "retry_params=(-X)" in script
    assert 'RETRY_FLAG=""' not in script
    assert '"$RETRY_FLAG"' not in script


def test_trace_dependency_install_is_nonblocking() -> None:
    for action_name in ("build", "test"):
        step = _step(_action(action_name), "Install trace dependencies")

        assert step["continue-on-error"] is True
        assert step["run"].strip() == "pip install -r .github/scripts/requirements.txt"


def test_build_trace_renderer_is_nonblocking() -> None:
    step = _step(_action("build"), "Render build trace")

    assert step["continue-on-error"] is True
