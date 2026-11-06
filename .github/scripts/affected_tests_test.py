import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

from . import affected_tests as affected


def node(uid, suite="tests/a/unittest", platform="linux"):
    return {
        "uid": uid,
        "platform": platform,
        "kv": {"run_test_node": True, "path": suite},
    }


def graph_suites(tmp_path, nodes):
    path = tmp_path / "graph.json"
    path.write_text(json.dumps({"graph": nodes}))
    return affected.read_suites(path)


def test_split_suite_changes_and_target_expansion(tmp_path):
    base = graph_suites(
        tmp_path,
        [
            node("chunk0"),
            node("chunk1"),
            node("style", "tests/a/clang_tidy"),
            node("child", "tests/a/child/unittest"),
            node("deleted", "tests/deleted/unittest"),
        ],
    )
    head = graph_suites(
        tmp_path,
        [
            node("chunk0"),
            node("changed-chunk1"),
            node("style", "tests/a/clang_tidy"),
            node("child", "tests/a/child/unittest"),
            node("new", "tests/new/unittest"),
            {"uid": "compile", "kv": {"p": "CC"}},
        ],
    )
    result = affected.compare_suites(base, head)
    assert result["targets"] == ["tests/a", "tests/new"]
    assert [item["reason"] for item in result["selected_suites"]] == [
        "changed",
        "added",
    ]
    assert [item["suite"] for item in result["execution_suites"]] == [
        "tests/a/clang_tidy",
        "tests/a/unittest",
        "tests/new/unittest",
    ]
    assert result["removed_suites"][0]["suite"] == "tests/deleted/unittest"


def test_removed_chunk_selects_remaining_suite(tmp_path):
    base = graph_suites(tmp_path, [node("chunk0"), node("chunk1")])
    head = graph_suites(tmp_path, [node("chunk0")])
    assert affected.compare_suites(base, head)["targets"] == ["tests/a"]


@pytest.mark.parametrize("value", ["../ut", "/tmp/ut", "-X", "tests\nall", ""])
def test_rejects_invalid_target(value):
    with pytest.raises(ValueError):
        affected.target_path(value)


@pytest.mark.parametrize(
    "graph", [{}, {"graph": {}}, {"graph": [node("uid", "")]}, {"graph": [{}]}]
)
def test_invalid_graph_is_not_an_empty_selection(tmp_path, graph):
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(graph))
    with pytest.raises(ValueError):
        affected.read_suites(path)


# A fake ya exercises the real CLI, Git history, isolated checkouts, command
# boundaries and selection without compiling the repository in unit tests.
FAKE_YA = """#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

VERSION = "base"
args = sys.argv[1:]
with open(os.environ["FAKE_YA_LOG"], "a") as log:
    log.write(json.dumps({"args": args, "cwd": os.getcwd(), "version": VERSION,
                          "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()}) + "\\n")
assert os.environ["YA_SOURCE_ROOT"] == os.getcwd()
targets = [arg.split("=", 1)[1] for arg in args if arg.startswith("--target=")]
assert targets
graph_path = next((arg.split("=", 1)[1] for arg in args if arg.startswith("--dump-graph-to-file=")), None)
if graph_path:
    assert "-j0" in args and "--cache-tests" in args and "--retest" not in args
    if Path(graph_path).name.startswith(os.environ.get("FAKE_GRAPH_FAILURE", "never")):
        sys.exit(17)
    nodes = []
    for suite, dependencies in json.loads(Path("suites.json").read_text()).items():
        target = suite.rsplit("/", 1)[0]
        if any(target == root or target.startswith(root + "/") for root in targets):
            uid = hashlib.sha256(b"".join(Path(dep).read_bytes() for dep in dependencies)).hexdigest()
            nodes.append({"uid": suite + uid, "kv": {"run_test_node": True, "path": suite}})
    Path(graph_path).write_text(json.dumps({"graph": nodes}))
else:
    raise AssertionError("The graph helper must never build or execute tests")
"""


def commit(repo, message):
    affected.git(repo, "add", ".")
    affected.git(repo, "commit", "-qm", message)
    return affected.resolve_commit(repo, "HEAD")


@pytest.fixture
def repository(tmp_path, monkeypatch):
    repo = tmp_path / "app checkout"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    affected.git(repo, "config", "user.email", "test@example.invalid")
    affected.git(repo, "config", "user.name", "Test")
    for target in ("tests", "tests/a", "tests/b", "tests/unchanged"):
        (repo / target).mkdir(parents=True, exist_ok=True)
        (repo / target / "ya.make").write_text("# test fixture\n")
    (repo / "shared.h").write_text("base")
    (repo / "unrelated.h").write_text("base")
    (repo / "suites.json").write_text(
        json.dumps(
            {
                "tests/a/unittest": ["shared.h"],
                "tests/b/unittest": ["shared.h"],
                "tests/unchanged/unittest": ["unrelated.h"],
            }
        )
    )
    (repo / "ya").write_text(FAKE_YA)
    commit(repo, "base")
    affected.git(repo, "checkout", "-qb", "feature")
    log = tmp_path / "invocations.jsonl"
    monkeypatch.setenv("FAKE_YA_LOG", str(log))
    return repo, log


def invoke(repo, output, *extra):
    return affected.main(
        [
            "--repo",
            str(repo),
            "--base-ref=main",
            "--targets=tests",
            "--output",
            str(output),
            *extra,
        ]
    )


def invocations(log):
    return [json.loads(line) for line in log.read_text().splitlines()]


def test_branch_diff_uses_merge_base_and_keeps_callers_checkout(repository, tmp_path):
    repo, log = repository
    base = affected.resolve_commit(repo, "main")
    (repo / "shared.h").write_text("feature change")
    # The head launcher must also be used to generate the base graph.
    (repo / "ya").write_text(FAKE_YA.replace('VERSION = "base"', 'VERSION = "head"'))
    head = commit(repo, "change shared dependency")
    affected.git(repo, "checkout", "-q", "main")
    (repo / "unrelated.h").write_text("main advanced")
    main = commit(repo, "unrelated main change")
    affected.git(repo, "checkout", "-q", "feature")
    (repo / "shared.h").write_text("uncommitted local work")
    (repo / "untracked.txt").write_text("keep me")
    before = affected.git(repo, "status", "--porcelain")
    output = tmp_path / "results"
    assert (
        invoke(repo, output, "--", "--build=relwithdebinfo", "--test-size=small") == 0
    )
    report = json.loads((output / "selection.json").read_text())
    assert report["base_commit"] == base
    assert report["base_ref_commit"] == main
    assert report["head_commit"] == head
    assert report["targets"] == ["tests/a", "tests/b"]
    assert report["status"] == "selected"
    calls = invocations(log)
    assert [call["commit"] for call in calls] == [base, head]
    assert all("--build=relwithdebinfo" in call["args"] for call in calls)
    assert all("--test-size=small" in call["args"] for call in calls)
    assert {call["version"] for call in calls} == {"head"}
    assert len({call["cwd"] for call in calls}) == 1
    assert not Path(calls[0]["cwd"]).exists()
    assert "--target=tests/unchanged" not in calls[-1]["args"]
    assert affected.git(repo, "status", "--porcelain") == before
    assert affected.git(repo, "symbolic-ref", "--short", "HEAD") == "feature"
    assert (repo / "shared.h").read_text() == "uncommitted local work"


def test_unchanged_branch_never_runs_all_tests(repository, tmp_path):
    repo, log = repository
    (repo / "README").write_text("documentation only")
    commit(repo, "docs")
    output = tmp_path / "results"
    assert invoke(repo, output) == 0
    assert len(invocations(log)) == 2
    assert (output / "targets.txt").read_text() == ""
    assert (
        json.loads((output / "selection.json").read_text())["status"]
        == "no affected tests"
    )


@pytest.mark.parametrize("failure", ["base", "head"])
def test_graph_failure_stops_before_execution(
    repository, tmp_path, monkeypatch, failure
):
    repo, log = repository
    monkeypatch.setenv("FAKE_GRAPH_FAILURE", failure)
    output = tmp_path / "results"
    assert invoke(repo, output) == 1
    assert all("-j0" in call["args"] for call in invocations(log))
    assert not (output / "selection.json").exists()
    assert not Path(invocations(log)[0]["cwd"]).exists()


def test_selection_only_with_added_target(repository, tmp_path):
    repo, log = repository
    (repo / "new").mkdir()
    (repo / "new/ya.make").touch()
    (repo / "suites.json").write_text(json.dumps({"new/unittest": ["shared.h"]}))
    commit(repo, "new target")
    output = tmp_path / "results"
    assert invoke(repo, output, "--targets=new") == 0
    report = json.loads((output / "selection.json").read_text())
    assert report["targets"] == ["new"]
    assert report["selected_suites"][0]["reason"] == "added"
    assert len(invocations(log)) == 1


def test_missing_target_does_not_silently_pass(repository, tmp_path):
    repo, _ = repository
    assert invoke(repo, tmp_path / "results", "--targets=tests,typo") == 1


@pytest.mark.parametrize("build_preset", ["relwithdebinfo", "release-tsan"])
@pytest.mark.parametrize(
    "enabled,changed,selection_only,graph_failure",
    [
        (False, False, False, False),
        (True, True, False, False),
        (True, False, False, False),
        (True, True, True, False),
        (True, True, False, True),
    ],
)
def test_test_action_selects_before_existing_execution(
    repository,
    tmp_path,
    monkeypatch,
    build_preset,
    enabled,
    changed,
    selection_only,
    graph_failure,
):
    repo, log = repository
    if changed:
        (repo / "shared.h").write_text("changed dependency")
        commit(repo, "change")
    if graph_failure:
        monkeypatch.setenv("FAKE_GRAPH_FAILURE", "head")
    action_path = Path(__file__).parents[1] / "actions/test/action.yaml"
    action = yaml.safe_load(action_path.read_text())
    step = next(
        step
        for step in action["runs"]["steps"]
        if step.get("id") == "check_test_results"
    )
    # Run the actual action shell and helper against a real Git history, then
    # inspect the selected invocation before cache setup and the existing test loop.
    prepare_args = step["run"].split('if [ "$USE_NETWORK_CACHE" == "yes" ]; then')[0]
    github_output = tmp_path / "github-output"
    result = subprocess.run(
        [
            "bash",
            "-euc",
            prepare_args
            + '\nprintf "__ARGS__\\0"; printf "%s\\0" "${extra_params[@]}"',
        ],
        cwd=Path(__file__).parents[2],
        env={
            **os.environ,
            "GITHUB_OUTPUT": str(github_output),
            "GITHUB_WORKSPACE": str(repo),
            "AFFECTED_TESTS_DIR": str(tmp_path / "selection"),
            "AFFECTED_BASE_REF": "main",
            "TEST_TARGET": "tests",
            "TEST_SIZE": "small,medium",
            "TEST_TYPE": "unittest",
            "AFFECTED_TESTS": str(enabled).lower(),
            "SELECTION_ONLY": str(selection_only).lower(),
            "BUILD_PRESET": build_preset,
            "TEST_THREADS": "2",
            "LINK_THREADS": "1",
            "OUT_DIR": "/tmp/unused-test-output",
        },
        capture_output=True,
        text=True,
    )
    outputs = github_output.read_text()
    if graph_failure:
        assert result.returncode != 0
        assert "__ARGS__" not in result.stdout
        assert "failed_tests=no" not in outputs
        return
    assert result.returncode == 0, result.stderr
    if enabled:
        calls = invocations(log)
        assert len(calls) == 2
        assert all("--test-size=small" in call["args"] for call in calls)
        assert all("--test-size=medium" in call["args"] for call in calls)
        assert all("--test-type=unittest" in call["args"] for call in calls)
        if build_preset == "release-tsan":
            assert all("--sanitize=thread" in call["args"] for call in calls)
        if not changed or selection_only:
            assert "__ARGS__" not in result.stdout
            assert "failed_tests=no" in outputs
            assert ("no_affected_tests=true" in outputs) == (not changed)
            return
    else:
        assert not log.exists()
    args = result.stdout.split("__ARGS__\0", 1)[1].split("\0")
    assert ("--ignore-recurses" in args) == enabled
    assert "--retest" in args
    assert [arg for arg in args if arg.startswith("--target=")] == (
        ["--target=tests/a", "--target=tests/b"] if enabled else ["--target=tests"]
    )
