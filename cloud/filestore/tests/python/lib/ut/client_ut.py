import time

import pytest
import yatest.common as common

from cloud.filestore.tests.python.lib import client as client_module
from cloud.storage.core.tools.common.python import daemon as daemon_module


@pytest.fixture
def launch_client(monkeypatch, tmp_path):
    processes = []
    streams = []
    execute_process = daemon_module.common.execute

    def launch(script, timeout=5, finish_before_return=False):
        def execute(command, **kwargs):
            streams.extend([kwargs["stdin"], kwargs["stdout"], kwargs["stderr"]])
            execution = execute_process(
                [common.python_path(), "-c", script],
                **kwargs)
            processes.append(execution.process)
            if finish_before_return:
                execution.process.wait()
            return execution

        monkeypatch.setattr(daemon_module.common, "execute", execute)
        return client_module.FilestoreCliClient(
            "unused-client", 1, cwd=str(tmp_path), timeout=timeout)

    yield launch, processes, streams

    for process in processes:
        if process.poll() is None:
            process.kill()
        process.wait()


@pytest.mark.parametrize("exit_code", [0, 1])
def test_mount_reports_early_exit(launch_client, tmp_path, exit_code):
    launch, processes, streams = launch_client
    client = launch(
        "import sys; sys.stderr.write('mount diagnostic\\n'); "
        "sys.exit({})".format(exit_code), timeout=30)

    started = time.monotonic()
    with pytest.raises(RuntimeError) as error:
        client.mount("test-fs", str(tmp_path / "mount"))

    assert time.monotonic() - started < 20
    message = str(error.value)
    assert "Process exit_code = {}".format(exit_code) in message
    assert "mount diagnostic" in str(error.value)
    assert "nfs-mnt-test-fs.err" in str(error.value)
    assert processes[0].returncode == exit_code
    assert all(stream.closed for stream in streams)


def test_mount_timeout_stops_client(launch_client, tmp_path):
    launch, processes, streams = launch_client
    client = launch("import time; time.sleep(60)", timeout=0.01)

    with pytest.raises(RuntimeError, match="mount was not ready") as error:
        client.mount("test-fs", str(tmp_path / "mount"))

    assert "client pid {}".format(processes[0].pid) in str(error.value)
    assert processes[0].poll() is not None
    assert all(stream.closed for stream in streams)


@pytest.mark.parametrize("finish_before_return", [False, True])
def test_mount_error_limits_stderr_to_100_log_lines(launch_client, tmp_path, finish_before_return):
    launch, processes, streams = launch_client
    client = launch(
        "import sys; sys.stderr.write('startup log\\n' * 150 + 'mount failed\\n'); sys.exit(1)",
        finish_before_return=finish_before_return)

    with pytest.raises(RuntimeError) as error:
        client.mount("test-fs", str(tmp_path / "mount"))

    assert "Stderr tail:" in str(error.value)
    assert "mount failed" in str(error.value)
    assert str(error.value).splitlines().count("startup log") == 99
    assert processes[0].returncode == 1
    assert all(stream.closed for stream in streams)


def test_mount_success_keeps_client_running(launch_client, monkeypatch, tmp_path):
    launch, processes, streams = launch_client
    client = launch("import time; time.sleep(60)")
    monkeypatch.setattr(client_module.os.path, "ismount", lambda path: True)

    assert client.mount("test-fs", str(tmp_path / "mount")) == processes[0].pid
    assert processes[0].poll() is None
    assert all(stream.closed for stream in streams)


def test_mount_readiness_error_stops_client(launch_client, monkeypatch, tmp_path):
    launch, processes, streams = launch_client
    client = launch("import time; time.sleep(60)")

    def fail_readiness(path):
        raise OSError("readiness check failed")

    monkeypatch.setattr(client_module.os.path, "ismount", fail_readiness)
    with pytest.raises(OSError, match="readiness check failed"):
        client.mount("test-fs", str(tmp_path / "mount"))

    assert processes[0].poll() is not None
    assert all(stream.closed for stream in streams)


def test_mount_timeout_kills_client_ignoring_sigterm(launch_client, monkeypatch, tmp_path):
    launch, processes, streams = launch_client
    ready = tmp_path / "client-ready"
    client = launch(
        "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "open({!r}, 'w').close(); time.sleep(60)".format(str(ready)), timeout=0.1)

    def not_mounted(path):
        assert client_module.wait_for(ready.exists, timeout_seconds=10)
        return False

    monkeypatch.setattr(client_module.os.path, "ismount", not_mounted)

    with pytest.raises(RuntimeError, match="mount was not ready"):
        client.mount("test-fs", str(tmp_path / "mount"))

    assert processes[0].returncode == -9
    assert all(stream.closed for stream in streams)


@pytest.mark.parametrize("finish_before_return", [False, True])
def test_mount_failure_runs_coredump_and_sanitizer_checks(
        launch_client, monkeypatch, tmp_path, finish_before_return):
    launch, processes, streams = launch_client
    checked = []
    execution_type = common.process._Execution
    monkeypatch.setattr(execution_type, "verify_no_coredumps",
                        lambda self: checked.append("coredumps"))
    monkeypatch.setattr(execution_type, "verify_sanitize_errors",
                        lambda self: checked.append("sanitizers"))
    client = launch("import sys; sys.exit(1)", timeout=30,
                    finish_before_return=finish_before_return)

    started = time.monotonic()
    with pytest.raises(RuntimeError):
        client.mount("test-fs", str(tmp_path / "mount"))

    assert time.monotonic() - started < 20
    assert "coredumps" in checked
    assert "sanitizers" in checked
    assert processes[0].poll() == 1
    assert all(stream.closed for stream in streams)
