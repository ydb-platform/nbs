import importlib.util
import logging
import os
from types import SimpleNamespace

import pytest
import yatest.common as common


@pytest.fixture
def recipe(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location(
        "mount_recipe", common.source_path(
            "cloud/filestore/tests/recipes/mount/__main__.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module.common, "work_path", lambda path: str(tmp_path / path))
    monkeypatch.setattr(module.common, "binary_path", lambda path: path)
    monkeypatch.setattr(module.common, "output_path", lambda path="": str(tmp_path / path))
    monkeypatch.setattr(module.os, "system", lambda command: 0)
    return module


def test_failed_second_mount_preserves_first_for_cleanup(recipe, monkeypatch):
    class Client:
        def __init__(self, **kwargs):
            pass

        def create(self, *args, **kwargs):
            pass

        def mount(self, filesystem, path):
            if path.endswith("second"):
                raise RuntimeError("second mount failed")
            return 123

    monkeypatch.setattr(recipe, "FilestoreCliClient", Client)
    stopped = []
    monkeypatch.setattr(recipe, "shutdown", stopped.append)
    monkeypatch.setattr(recipe.common, "execute", lambda *args, **kwargs: SimpleNamespace(
        process=SimpleNamespace(poll=lambda: 0)))

    with pytest.raises(RuntimeError, match="second mount failed"):
        recipe.start(["--mount-path", "first", "--mount-path", "second"])
    recipe.stop([])

    assert stopped == [123]


def test_stop_without_startup_state(recipe, monkeypatch):
    def unexpected_shutdown(pid):
        pytest.fail("No client was started")

    monkeypatch.setattr(recipe, "shutdown", unexpected_shutdown)
    recipe.stop([])


def test_stop_clients_skips_blank_lines(recipe, monkeypatch, tmp_path):
    (tmp_path / recipe.PID_FILE_NAME).write_text("\n123\n  \n456\n\n")
    stopped = []
    monkeypatch.setattr(recipe, "shutdown", stopped.append)

    recipe.stop_clients()

    assert stopped == [123, 456]


@pytest.mark.parametrize("unblocks_on_shutdown", [False, True])
def test_stop_shuts_down_clients_after_shared_listing_deadline(
        recipe, monkeypatch, tmp_path, caplog, unblocks_on_shutdown):
    events = []
    listings = []
    clock = [0.0]

    def sleep(seconds):
        clock[0] += seconds

    monkeypatch.setattr(recipe, "time", SimpleNamespace(
        monotonic=lambda: clock[0], sleep=sleep))

    class Listing:
        def __init__(self, path, output):
            self.path = path
            self.output = output
            # Model the child's descriptor, which outlives the parent's stream.
            self.child_output = os.fdopen(os.dup(output.fileno()), "wb")
            self.process = self
            self.pid = 1000 + len(listings)
            self.returncode = 0 if path == "finished" else None

        def poll(self):
            return self.returncode

        def kill(self):
            assert clock[0] == pytest.approx(5)
            events.append(("kill", self.path))
            # Simulate SIGKILL pending on an uninterruptible FUSE request.
            assert self.returncode is None

        def wait(self, **kwargs):
            pytest.fail("Listing wait must not block client shutdown")

    def execute(command, **kwargs):
        assert command[:2] == ["ls", "-la"]
        assert kwargs["wait"] is False
        kwargs["stdout"].write(b"listing diagnostic\n")
        kwargs["stdout"].flush()
        events.append(("start", command[2]))
        listing = Listing(command[2], kwargs["stdout"])
        listings.append(listing)
        return listing

    def shutdown(pid):
        assert clock[0] == pytest.approx(5)
        assert events == [
            ("start", "finished"), ("start", "blocked-1"), ("start", "blocked-2"),
            ("kill", "blocked-1"), ("kill", "blocked-2")]
        assert listings[1].poll() is None
        assert listings[2].poll() is None
        events.append(("stop", pid))
        for listing in listings[1:]:
            assert listing.output.closed
            listing.child_output.write(b"diagnostic during client shutdown\n")
            listing.child_output.flush()
            if unblocks_on_shutdown:
                listing.returncode = -9

    (tmp_path / recipe.MOUNT_PATH_FILE_NAME).write_text("finished\nblocked-1\nblocked-2\n")
    (tmp_path / recipe.PID_FILE_NAME).write_text("123\n")
    monkeypatch.setattr(recipe.common, "execute", execute)
    monkeypatch.setattr(recipe, "shutdown", shutdown)
    caplog.set_level(logging.INFO, logger=recipe.logger.name)

    try:
        recipe.stop(["--filesystem", "custom-fs"])
    finally:
        for listing in listings:
            listing.child_output.close()

    assert events == [
        ("start", "finished"), ("start", "blocked-1"), ("start", "blocked-2"),
        ("kill", "blocked-1"), ("kill", "blocked-2"), ("stop", 123)]
    assert clock[0] == pytest.approx(5)
    assert all(listing.output.closed for listing in listings)
    assert "Mount listing tail:" in caplog.text
    assert "Stderr tail:" not in caplog.text
    for index, path in enumerate(("finished", "blocked-1", "blocked-2")):
        expected = "FileSystemId: custom-fs\nMount path: {}\n\nlisting diagnostic\n".format(tmp_path / path)
        if index:
            expected += "diagnostic during client shutdown\n"
        assert (tmp_path / "mount-ls-{}.log".format(index)).read_text() == expected
    assert ("still running after SIGKILL" in caplog.text) is not unblocks_on_shutdown


def test_create_failure_discards_previous_startup_state(recipe, monkeypatch, tmp_path):
    class Client:
        def __init__(self, **kwargs):
            pass

        def create(self, *args, **kwargs):
            raise RuntimeError("create failed")

    (tmp_path / recipe.PID_FILE_NAME).write_text("123\n")
    (tmp_path / recipe.MOUNT_PATH_FILE_NAME).write_text("old-mount\n")
    monkeypatch.setattr(recipe, "FilestoreCliClient", Client)
    monkeypatch.setattr(recipe, "shutdown", lambda pid: pytest.fail("Stale PID used"))
    monkeypatch.setattr(recipe.common, "execute", lambda *args, **kwargs: pytest.fail("Stale mount inspected"))
    with pytest.raises(RuntimeError, match="create failed"):
        recipe.start([])
    recipe.stop([])
    assert not (tmp_path / recipe.PID_FILE_NAME).exists()
    assert not (tmp_path / recipe.MOUNT_PATH_FILE_NAME).exists()
