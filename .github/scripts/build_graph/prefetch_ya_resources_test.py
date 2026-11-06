import copy
import json
from pathlib import Path
import subprocess

import pytest

from . import prefetch_ya_resources as prefetch


def fixture_graph():
    return {
        "conf": {
            "resources": [
                {"pattern": "COMPILER", "resource": "sbr:9100001"},
                {"pattern": "UNUSED", "resource": "sbr:9100003"},
                {"pattern": "VCS", "resource": "base64:revision:dGVzdA=="},
            ]
        },
        "graph": [
            {
                "uid": "compile",
                "deps": [],
                "inputs": ["$(SOURCE_ROOT)/source.cc"],
                "outputs": ["$(BUILD_ROOT)/program"],
                "cmds": [{"cmd_args": ["$(COMPILER)/compiler", "$(VCS)/revision"]}],
                "resources": [{"uri": "sbr:9100002"}],
            },
            {
                "uid": "test",
                "deps": ["compile"],
                "inputs": [],
                "outputs": [],
                "cmd_args": ["$(BUILD_ROOT)/program"],
            },
            {
                "uid": "unreachable",
                "deps": [],
                "inputs": ["$(UNUSED)/tool"],
                "resources": [{"uri": "sbr:9100003"}],
            },
        ],
        "result": ["test"],
    }


def test_only_reachable_resources_survive_and_no_original_commands():
    graph = fixture_graph()
    original = copy.deepcopy(graph)
    plan, summary = prefetch.make_prefetch_graph([graph])
    assert graph == original
    assert summary == {
        "reachable_nodes": 2,
        "tool_requirements": 1,
        "resource_requirements": 1,
        "local_tool_patterns_skipped": ["VCS"],
    }
    assert len(plan["graph"]) == 2
    for node in plan["graph"]:
        assert node["cmds"][0]["cmd_args"][0] == "/bin/true"
        assert node["deps"] == node["inputs"] == node["outputs"] == []
        assert node["cache"] is False  # Recheck native caches even after eviction.
    assert "source.cc" not in json.dumps(plan)
    assert "9100003" not in json.dumps(plan)
    assert plan["conf"]["resources"][0]["resource"] == "sbr:9100001"
    assert {n["uid"] for n in plan["graph"]} == set(plan["result"])


def test_union_deduplicates_resources_without_confusing_tool_platforms():
    first = fixture_graph()
    second = fixture_graph()
    second["conf"]["resources"][0]["resource"] = "sbr:9200001"
    second["conf"]["resources"][0]["strip_prefix"] = 1
    plan, summary = prefetch.make_prefetch_graph([first, second, first])
    assert summary["tool_requirements"] == 2
    assert summary["resource_requirements"] == 1
    assert len({r["pattern"] for r in plan["conf"]["resources"]}) == 2
    assert any(r.get("strip_prefix") == 1 for r in plan["conf"]["resources"])
    # Order of input graphs must not change the cache-warming plan.
    assert plan == prefetch.make_prefetch_graph([second, first])[0]


def test_tool_references_in_inputs_and_environment_and_ext_resources():
    graph = fixture_graph()
    graph["graph"][0].update(
        cmds=[],
        inputs=["$(COMPILER)/include"],
        env={"TOOLS": "$(UNUSED)/bin"},
        resources=[{"uri": "ext:fixtures/input.external"}],
    )
    variants = [
        {"platform": "linux", "resource": "sbr:9100001"},
        {"platform": "darwin", "resource": "sbr:9200001"},
    ]
    graph["conf"]["resources"][0] = {"pattern": "COMPILER", "resources": variants}
    plan, summary = prefetch.make_prefetch_graph([graph])
    assert summary["tool_requirements"] == 2
    assert any(r.get("resources") == variants for r in plan["conf"]["resources"])
    assert any(
        n["resources"] == [{"uri": "ext:fixtures/input.external"}]
        for n in plan["graph"]
    )


def test_incomplete_graph_fails_instead_of_claiming_complete_prefetch():
    graph = fixture_graph()
    graph["graph"][0]["deps"] = ["missing"]
    with pytest.raises(ValueError, match="Missing graph node: missing"):
        prefetch.make_prefetch_graph([graph])


def test_conflicting_resource_descriptions_fail():
    first = fixture_graph()
    second = fixture_graph()
    second["graph"][0]["resources"][0]["extra"] = "different"
    with pytest.raises(ValueError, match="Conflicting descriptions"):
        prefetch.make_prefetch_graph([first, second])


@pytest.mark.parametrize(
    "argument", ["-j8", "--threads=8", "--dist", "--build-custom-json=x", "--"]
)
def test_caller_cannot_override_graph_only_phase(argument):
    with pytest.raises(ValueError):
        prefetch.validate_make_args([argument])


def test_cli_separates_graph_generation_from_resource_execution(tmp_path, monkeypatch):
    calls = []

    def run(command, repo, env):
        calls.append(command)
        assert repo == tmp_path
        assert env["YA_CACHE_DIR"] == str(tmp_path / "cache")
        assert env["YA_SOURCE_ROOT"] == str(tmp_path)
        if len(calls) == 1:
            assert "-j0" in command
            assert "-A" in command and "cloud/storage" in command
            output = next(
                x.split("=", 1)[1]
                for x in command
                if x.startswith("--dump-graph-to-file=")
            )
            Path(output).write_text(json.dumps(fixture_graph()))
        else:
            assert "-A" not in command and "cloud/storage" not in command
            assert "-j3" in command
            path = next(
                x.split("=", 1)[1]
                for x in command
                if x.startswith("--build-custom-json=")
            )
            assert all(
                n["cmds"][0]["cmd_args"][0] == "/bin/true"
                for n in json.loads(Path(path).read_text())["graph"]
            )

    monkeypatch.setattr(prefetch, "run", run)
    assert (
        prefetch.main(
            [
                "--repo-root",
                str(tmp_path),
                "--cache-dir",
                str(tmp_path / "cache"),
                "--save-plan",
                str(tmp_path / "plan.json"),
                "--jobs",
                "3",
                "--",
                "-A",
                "--build=release",
                "cloud/storage",
            ]
        )
        == 0
    )
    assert len(calls) == 2
    assert (tmp_path / "plan.json").is_file()


def test_fetch_failure_is_reported(tmp_path, monkeypatch):
    source = tmp_path / "graph.json"
    source.write_text(json.dumps(fixture_graph()))

    def fail(command, _repo, _env):  # noqa: U101
        raise subprocess.CalledProcessError(7, command)

    monkeypatch.setattr(prefetch, "run", fail)
    assert prefetch.main(["--graph-json", str(source)]) == 7
