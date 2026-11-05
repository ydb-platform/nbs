import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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


@pytest.mark.skipif(
    not os.environ.get("YA_PREFETCH_INTEGRATION_BINARY"),
    reason="Set YA_PREFETCH_INTEGRATION_BINARY to a local ya-bin for the native cache test",
)
def test_native_downloads_are_reused_by_normal_build_offline(tmp_path, request):
    """Exercise the real ya fetchers, both mapping files, and persistent resource cache."""
    repo = tmp_path / "repo"
    (repo / "build").mkdir(parents=True)
    (repo / ".arcadia.root").touch()
    (repo / "ya.conf").write_text("tools_cache = false\nbuild_cache = false\n")
    marker = tmp_path / "compiler-was-run"
    archive = io.BytesIO()
    payload = f"#!/bin/sh\ntouch '{marker}'\nexit 1\n".encode()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        entry = tarfile.TarInfo("compiler")
        entry.mode = 0o755
        entry.size = len(payload)
        tar.addfile(entry, io.BytesIO(payload))
    responses = {"/tool": archive.getvalue(), "/data": b"test resource contents\n"}
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            content = responses.get(self.path)
            self.send_response(200 if content is not None else 404)
            self.send_header("Content-Length", str(len(content or b"")))
            self.end_headers()
            self.wfile.write(content or b"")

        def log_message(self, *_args):  # noqa: U101
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    url = f"http://127.0.0.1:{server.server_port}"
    (repo / "build/mapping.conf.json").write_text(
        json.dumps(
            {
                "resources": {"9100001": "{registry_endpoint}/tool"},
                "extensions": {"registry_endpoint": url},
            }
        )
    )
    (repo / "build/ext_mapping.conf.json").write_text(
        json.dumps({"resources": {"9100002": f"{url}/data"}})
    )
    graph_file = tmp_path / "graph.json"
    graph_file.write_text(json.dumps(fixture_graph()))
    cache = tmp_path / "cache"
    binary = os.environ["YA_PREFETCH_INTEGRATION_BINARY"]
    env = os.environ.copy()
    for name in (
        "YA_CACHE_DIR_TOOLS",
        "YA_TOOLS_CONFIG_PATH",
        "YA_TOOLS_EXT_CONFIG_FILE",
    ):
        env.pop(name, None)
    env.update(
        YA_CACHE_DIR=str(cache), YA_SOURCE_ROOT=str(repo), YA_AC="no", YA_TC="no"
    )
    common_args = ["--no-local-executor"]
    cache_binary = os.environ.get("YA_PREFETCH_INTEGRATION_TC_BINARY")
    if cache_binary:
        common_args += [
            "--ya-tc",
            "--ya-ac",
            "--ya-tc-master",
            "--ya-ac-master",
            "--ya-gl-conf=service=all",
            "--cache-size=32GiB",
            "--tools-cache-size=12GiB",
            "--ya-ac-conf=disk_limit=34359738368",
            f"--ya-tc-bin={cache_binary}",
        ]

        def stop_cache():
            result = subprocess.run(
                [
                    cache_binary,
                    "--halt",
                    "--grpc-service=all",
                    f"--tools_cache-lock_file={cache / 'tools/v4/.cache_lock'}",
                ],
                env=env,
                capture_output=True,
                timeout=15,
                check=False,
            )
            assert result.returncode == 0, result.stdout + result.stderr

        request.addfinalizer(stop_cache)
    command = [
        sys.executable,
        prefetch.__file__,
        "--repo-root",
        str(repo),
        "--ya-bin",
        binary,
        "--cache-dir",
        str(cache),
        *(f"--ya-arg={argument}" for argument in common_args),
        "--graph-json",
        str(graph_file),
    ]

    def execute(cmd):
        result = subprocess.run(
            cmd, env=env, cwd=repo, capture_output=True, text=True, timeout=45
        )
        assert result.returncode == 0, result.stdout + result.stderr

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        execute(command)
        assert sorted(requests) == ["/data", "/tool"]
        assert not marker.exists()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    if cache_binary:
        # A new VM must reuse the on-disk cache after the old daemon exits.
        stop_cache()

    # An ordinary graph uses the original pattern name and native resource UID.
    # Include ext: as well: it resolves to the already warmed sbr: resource.
    (repo / "input.external").write_text(
        json.dumps({"storage": "SANDBOX", "resource_id": 9100002})
    )
    consumer = {
        "conf": {"resources": [{"pattern": "COMPILER", "resource": "sbr:9100001"}]},
        "graph": [
            prefetch.noop_node(
                "ordinary-consumer",
                ["$(COMPILER)/compiler"],
                [
                    {"uri": "sbr:9100002"},
                    {"uri": "ext:input.external"},
                ],
            )
        ],
        "result": ["ordinary-consumer"],
    }
    consumer_file = tmp_path / "consumer.json"
    consumer_file.write_text(json.dumps(consumer))
    execute(
        [
            binary,
            "make",
            f"--build-custom-json={consumer_file}",
            "-j2",
            *common_args,
            "--no-src-links",
            f"--output={tmp_path / 'output'}",
        ]
    )
    execute(command)
    assert not marker.exists()
    assert sorted(requests) == ["/data", "/tool"]
