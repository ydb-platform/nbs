import json
import os
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest

from . import prefetch_ya_resources as prefetch

PROVISIONER = (
    Path(__file__).resolve().parents[2] / "packer/scripts/prefetch-ya-cache.sh"
)
FAKE_YA = """\
#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
with open(os.environ["PREFETCH_TEST_LOG"], "a") as log:
    log.write(json.dumps({
        "args": args,
        "revision": Path("revision-marker").read_text(),
        "cache": os.environ["YA_CACHE_DIR"],
    }) + "\\n")
assert args[0] == "make", "Provisioning must let ya manage its cache daemons"
for arg in args:
    if arg.startswith("--dump-graph-to-file="):
        Path(arg.split("=", 1)[1]).write_text(json.dumps({
            "conf": {"resources": [{"pattern": "TOOL", "resource": "sbr:1"}]},
            "graph": [{"uid": "compile", "deps": [], "inputs": ["$(TOOL)/cc"],
                       "cmds": [{"cmd_args": ["/must-not-run"]}],
                       "resources": [{"uri": "sbr:2"}]}],
            "result": ["compile"],
        }))
    if arg.startswith("--build-custom-json="):
        plan = json.loads(Path(arg.split("=", 1)[1]).read_text())
        assert all(n["cmds"][0]["cmd_args"][0] == "/bin/true" for n in plan["graph"])
        sys.exit(int(os.environ.get("PREFETCH_TEST_MAKE_EXIT", "0")))
"""


@pytest.fixture
def image_checkout(tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()

    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(remote), *args], text=True, stderr=subprocess.PIPE
        ).strip()

    git("init")
    git("config", "user.name", "Prefetch test")
    git("config", "user.email", "prefetch@example.invalid")
    git("config", "commit.gpgsign", "false")
    script = remote / ".github/scripts/build_graph/prefetch_ya_resources.py"
    script.parent.mkdir(parents=True)
    shutil.copyfile(prefetch.__file__, script)
    (remote / "ya").write_text(textwrap.dedent(FAKE_YA))
    (remote / "ya").chmod(0o755)
    (remote / "build").mkdir()
    for name in ("mapping.conf.json", "ext_mapping.conf.json"):
        (remote / "build" / name).write_text("{}\n")
    (remote / "revision-marker").write_text("pinned")
    git("add", ".")
    git("commit", "-m", "Pinned revision")
    revision = git("rev-parse", "HEAD")
    (remote / "revision-marker").write_text("newer")
    git("commit", "-am", "Newer revision must not be used")

    cache = tmp_path / "cache"
    for directory in ("tools", "build"):
        (cache / directory).mkdir(parents=True)
    (cache / "tools/retained").write_text("existing cache")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    log = tmp_path / "commands.jsonl"
    env = {
        **os.environ,
        "PREFETCH_REPOSITORY_URL": str(remote),
        "PREFETCH_REVISION": revision,
        "PREFETCH_CACHE_DIR": str(cache),
        "PREFETCH_TEST_LOG": str(log),
        "TMPDIR": str(scratch),
    }
    return env, cache, scratch, log


def test_packer_warms_pinned_revision_and_removes_checkout(image_checkout):
    env, cache, scratch, log = image_checkout
    result = subprocess.run(
        ["bash", str(PROVISIONER)], env=env, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr
    events = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(events) == 2  # Generate graph, prefetch.
    assert all(event["revision"] == "pinned" for event in events)
    assert all(event["cache"] == str(cache) for event in events)
    assert "-A" in events[0]["args"] and "-j0" in events[0]["args"]
    assert "cloud/fastshard" in events[0]["args"]
    assert "-A" not in events[1]["args"]
    assert (cache / "prefetch/revision.txt").read_text().strip() == env[
        "PREFETCH_REVISION"
    ]
    assert (cache / "prefetch/plan.json").is_file()
    assert "ext_mapping.conf.json" in (cache / "prefetch/mapping.sha256").read_text()
    assert (cache / "tools/retained").read_text() == "existing cache"
    assert list(scratch.iterdir()) == []


@pytest.mark.parametrize(
    ("override", "value", "expected_code"),
    [
        ("PREFETCH_TEST_MAKE_EXIT", "23", 23),
        ("PREFETCH_REVISION", "missing-revision", 128),
    ],
)
def test_packer_fails_and_cleans_checkout(
    image_checkout, override, value, expected_code
):
    env, cache, scratch, _ = image_checkout
    env[override] = value
    result = subprocess.run(
        ["bash", str(PROVISIONER)], env=env, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == expected_code, result.stdout + result.stderr
    assert not (cache / "prefetch/revision.txt").exists()
    assert (cache / "tools/retained").is_file()
    assert list(scratch.iterdir()) == []
