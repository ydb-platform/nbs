import logging
import subprocess
from io import StringIO
from types import SimpleNamespace
from unittest import mock

import pytest
import yatest.common as common

import cloud.storage.core.tools.common.python.daemon as daemon_module
from cloud.storage.core.tools.common.python.daemon import Daemon, DaemonError


def test_log_process_threads(caplog):
    pid = 42
    contents = {
        "/proc/42/task/43/status": "Name:\ttest-thread\nState:\tD (disk sleep)\n",
        "/proc/42/task/43/wchan": "io_schedule\n",
        "/proc/42/task/43/stack": "[<0>] io_schedule+0x1/0x2\n",
    }

    def open_proc_file(path, *args, **kwargs):
        return StringIO(contents[path])

    caplog.set_level(logging.WARNING, logger=daemon_module.__name__)
    with mock.patch.object(daemon_module.os, "listdir", return_value=["43"]), \
            mock.patch("builtins.open", side_effect=open_proc_file):
        daemon_module._log_process_threads(pid)

    assert "PID 42 has 1 thread(s)" in caplog.text
    assert "State:\tD (disk sleep)" in caplog.text
    assert "io_schedule" in caplog.text
    assert "io_schedule+0x1/0x2" in caplog.text


def test_kill_logs_threads_when_process_is_still_not_reapable(tmp_path):
    daemon = Daemon(commands=[["daemon"]], cwd=str(tmp_path))
    execution = mock.Mock()
    execution.process.pid = 42
    execution.process.poll.return_value = None

    kill_timeout = common.TimeoutError("kill timed out")
    execution.kill.side_effect = kill_timeout
    execution.process.wait.side_effect = subprocess.TimeoutExpired(
        execution.command,
        5)
    daemon._Daemon__process = execution

    with mock.patch.object(daemon_module, "_log_process_threads") as log_threads:
        with pytest.raises(common.TimeoutError) as error:
            daemon.kill()

    assert error.value is kill_timeout
    execution.process.wait.assert_called_once_with(timeout=5)
    log_threads.assert_called_once_with(42)
    execution.wait.assert_not_called()
    daemon._Daemon__files.close()


@pytest.mark.parametrize("separator", ["\n", "\r\n"])
@pytest.mark.parametrize("trailing_newline", [False, True])
def test_stderr_tail_contains_last_lines(tmp_path, separator, trailing_newline):
    log = tmp_path / "stderr"
    lines = ["  log line {}".format(index) for index in range(150)]
    log.write_bytes((separator.join(lines) + (separator if trailing_newline else "")).encode("utf-8"))

    assert daemon_module.extract_log_tail(str(log), 100) == ["Stderr tail:", ""] + lines[-100:]


def test_stderr_tail_limits_read_size(tmp_path):
    log = tmp_path / "stderr"
    log.write_bytes(b"x" * 1000000)
    bytes_read = []
    original_open = open

    class TrackedFile:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def read(self, size):
            bytes_read.append(size)
            return self.stream.read(size)

    with mock.patch.object(daemon_module, "open", side_effect=lambda *args, **kwargs: TrackedFile(
            original_open(*args, **kwargs)), create=True):
        tail = daemon_module.extract_log_tail(str(log), 100)

    assert tail[2].startswith("[truncated")
    assert sum(bytes_read) == daemon_module._MAX_LOG_TAIL_BYTES


@pytest.mark.parametrize("content", [
    b"", b"only line", b"only line\n", b"\n", b"invalid utf-8: \xff\n",
])
def test_stderr_tail_handles_short_logs(tmp_path, content):
    log = tmp_path / "stderr"
    log.write_bytes(content)
    assert daemon_module.extract_log_tail(str(log), 100) == (
        ["Stderr tail:", ""] + content.decode("utf-8", errors="replace").splitlines())


def test_stderr_tail_handles_long_line(tmp_path):
    log = tmp_path / "stderr"
    unit = "ошибка "
    last_line = unit * (daemon_module._MAX_LOG_TAIL_BYTES // len(unit.encode('utf-8')) - 2)
    log.write_bytes(("old line\n" + last_line + "\n").encode("utf-8"))
    assert daemon_module.extract_log_tail(str(log), 1) == ["Stderr tail:", "", last_line]


def test_stderr_tail_marks_truncated_line(tmp_path):
    log = tmp_path / "stderr"
    log.write_bytes(b"x" * 1000000)

    assert daemon_module.extract_log_tail(str(log), 1) == [
        "Stderr tail:",
        "",
        "[truncated to the last 64 KiB; first line may be incomplete]",
        "x" * (64 * 1024),
    ]


@pytest.mark.parametrize("trailing_newline", [False, True])
def test_stderr_tail_omits_marker_when_fragment_is_discarded(tmp_path, trailing_newline):
    log = tmp_path / "stderr"
    log.write_bytes(
        b"x" * daemon_module._MAX_LOG_TAIL_BYTES + b"\nA\nB" +
        (b"\n" if trailing_newline else b""))

    assert daemon_module.extract_log_tail(str(log), 2) == ["Stderr tail:", "", "A", "B"]


def test_log_head_preserves_whitespace(tmp_path):
    log = tmp_path / "stderr"
    log.write_bytes(b"  indented  \n\ttabbed\t\r\n  \n")

    assert daemon_module.extract_log_head(str(log), 3) == [
        "Stderr content:", "", "  indented  ", "\ttabbed\t", "  "]


def test_stderr_tail_disabled_without_opening_file():
    assert daemon_module.extract_log_tail("missing-file", 0) == []
    assert daemon_module.extract_log_tail(None, 100) == []


def test_log_tail_uses_requested_label(tmp_path):
    log = tmp_path / "listing"
    log.write_text("entry\n")

    assert daemon_module.extract_log_tail(str(log), 1, label="Mount listing") == [
        "Mount listing tail:", "", "entry"]
    assert daemon_module.extract_log_tail(str(tmp_path / "missing"), 1, label="Mount listing")[0].startswith(
        "Mount listing tail unavailable:")


def test_log_head_limits_read_size_and_marks_truncation(tmp_path):
    log = tmp_path / "stderr"
    log.write_bytes(b"x" * 1000000)
    bytes_read = []
    original_open = open

    class TrackedFile:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def read(self, size):
            bytes_read.append(size)
            return self.stream.read(size)

    with mock.patch.object(daemon_module, "open", side_effect=lambda *args, **kwargs: TrackedFile(
            original_open(*args, **kwargs)), create=True):
        head = daemon_module.extract_log_head(str(log), 1)

    assert head == [
        "Stderr content:",
        "",
        "x" * daemon_module._MAX_LOG_TAIL_BYTES,
        "[truncated to the first 64 KiB; last line may be incomplete]",
    ]
    assert bytes_read == [daemon_module._MAX_LOG_TAIL_BYTES]


@pytest.mark.parametrize("relative_stderr", [False, True])
def test_shared_stdout_and_stderr_preserve_both_streams(tmp_path, monkeypatch, relative_stderr):
    monkeypatch.chdir(tmp_path)
    log = tmp_path / "combined.log"
    log.write_text("old contents must be replaced\n")
    with Daemon(
            commands=[["unused"]], cwd=str(tmp_path), stdout_file=str(log),
            stderr_file="combined.log" if relative_stderr else str(log)) as daemon:
        subprocess.run(
            [common.python_path(), "-c",
             "import os; os.write(1, b'first\\n'); os.write(2, b'error\\n'); os.write(1, b'last\\n')"],
            stdout=daemon._Daemon__stdout_file,
            stderr=daemon._Daemon__stderr_file,
            check=True)

    assert log.read_text() == "first\nerror\nlast\n"
    assert daemon._Daemon__stdout_file.closed
    assert daemon._Daemon__stderr_file.closed


def test_context_closes_logs_and_stops_successful_process(tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    daemon = Daemon(
        commands=[[common.python_path(), "-c", "import time; time.sleep(60)"]],
        cwd=str(tmp_path), stdout_file=str(stdout), stderr_file=str(stderr))
    try:
        with daemon:
            daemon.start()
            process = daemon._Daemon__process
        assert not daemon.is_alive()
        assert process.process.poll() is not None
        assert daemon.returncode == process.process.returncode
        assert daemon._Daemon__process is None
        daemon.stop()
        daemon.kill()
        daemon.close()
        assert daemon._Daemon__stdout_file.closed
        assert daemon._Daemon__stderr_file.closed
    finally:
        process = daemon._Daemon__process
        if process is not None and process.process.poll() is None:
            process.process.kill()
        if process is not None:
            process.process.wait()


def test_detach_transfers_execution_and_preserves_logs(tmp_path):
    gate = tmp_path / "continue"
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    daemon = Daemon(
        commands=[[common.python_path(), "-c",
                   "import os, sys, time\n"
                   "while not os.path.exists(sys.argv[1]): time.sleep(0.01)\n"
                   "os.write(1, b'output after detach\\n')\n"
                   "os.write(2, b'error after detach\\n')\n", str(gate)]],
        cwd=str(tmp_path), stdout_file=str(stdout), stderr_file=str(stderr))
    execution = None
    try:
        with daemon:
            daemon.start()
            execution = daemon.detach()
        assert execution.running
        assert daemon._Daemon__stdout_file.closed
        assert daemon._Daemon__stderr_file.closed
        daemon.stop()
        daemon.kill()
        daemon.close()
        gate.touch()
        execution.wait(timeout=10)
        assert execution.returncode == 0
        assert stdout.read_text() == "output after detach\n"
        assert stderr.read_text() == "error after detach\n"
    finally:
        if execution is not None:
            if execution.running:
                execution.process.kill()
            execution.process.wait()
        daemon.close()


@pytest.mark.parametrize("action", ["close", "detach"])
def test_terminal_state_blocks_pending_restart(tmp_path, monkeypatch, action):
    timer_factory = mock.Mock()
    monkeypatch.setattr(daemon_module.threading, "Timer", timer_factory)
    daemon = Daemon(
        commands=[[common.python_path(), "-c", "import time; time.sleep(60)"]],
        cwd=str(tmp_path), restart_allowed=True, restart_interval=1)
    execution = None
    try:
        daemon.start()
        execution = daemon._Daemon__process
        callback = timer_factory.call_args.args[1]
        getattr(daemon, action)()
        timer_factory.return_value.cancel.assert_called_once_with()
        timer_factory.return_value.join.assert_not_called()
        start = mock.Mock(side_effect=AssertionError("unexpected restart"))
        monkeypatch.setattr(daemon, "_Daemon__start", start)
        callback()
        start.assert_not_called()
        with pytest.raises(RuntimeError, match="closed or detached"):
            daemon.start()
        with pytest.raises(RuntimeError, match="closed or detached"):
            daemon.allow_restart()
        with pytest.raises(RuntimeError, match="Cannot detach"):
            daemon.detach()
    finally:
        if execution is not None:
            if execution.running:
                execution.process.kill()
            execution.process.wait()
        daemon.close()


def test_detach_requires_started_process(tmp_path):
    with Daemon(commands=[["unused"]], cwd=str(tmp_path)) as daemon:
        with pytest.raises(RuntimeError, match="Cannot detach"):
            daemon.detach()


@pytest.mark.parametrize("method", ["stop", "kill"])
def test_stop_and_kill_allow_starting_again(tmp_path, method):
    daemon = Daemon(
        commands=[[common.python_path(), "-c", "import time; time.sleep(60)"]],
        cwd=str(tmp_path))
    try:
        daemon.start()
        execution = daemon._Daemon__process
        getattr(daemon, method)()
        assert execution.process.poll() is not None
        daemon.start()
        assert daemon.is_alive()
        assert daemon._Daemon__process is not execution
    finally:
        daemon.close()


def test_context_without_start_closes_files_and_preserves_error(tmp_path):
    daemon = Daemon(commands=[["unused"]], cwd=str(tmp_path))
    error = RuntimeError("startup failed")

    with pytest.raises(RuntimeError) as caught:
        with daemon:
            raise error

    assert caught.value is error
    assert daemon._Daemon__process is None
    assert daemon._Daemon__stdin_file.closed
    assert daemon._Daemon__stdout_file.closed
    assert daemon._Daemon__stderr_file.closed


def test_start_failure_includes_stderr_and_exit_code(tmp_path, monkeypatch):
    execute_process = daemon_module.common.execute
    execution = None

    def execute(*args, **kwargs):
        nonlocal execution
        execution = execute_process(*args, **kwargs)
        execution.process.wait()
        return execution

    monkeypatch.setattr(daemon_module.common, "execute", execute)
    daemon = Daemon(
        commands=[[common.python_path(), "-c",
                   "import sys; sys.stderr.write('startup failed\\n'); sys.exit(1)"]],
        cwd=str(tmp_path), stderr_on_error_lines=5, stderr_tail=True)

    with pytest.raises(DaemonError) as caught:
        with daemon:
            daemon.start()

    message = str(caught.value)
    assert "Process exit_code = 1" in message
    assert daemon.stderr_file_name in message
    assert "startup failed" in message
    assert execution.process.poll() == 1
    assert daemon._Daemon__stderr_file.closed


def test_ping_failure_includes_stderr(tmp_path, monkeypatch):
    stderr = tmp_path / "stderr"
    daemon = Daemon(
        commands=[["unused"]], cwd=str(tmp_path), stderr_file=str(stderr),
        ping_port=12345, ping_path="/ping", ping_success_codes=[200],
        ping_attempts=1, ping_timeout=0,
        stderr_on_error_lines=5, stderr_tail=True)
    execution = mock.Mock(returncode=None)
    execution.process.poll.return_value = None
    monkeypatch.setattr(daemon_module.common, "execute", lambda **kwargs: execution)
    monkeypatch.setattr(daemon_module.requests, "get", lambda endpoint: mock.Mock(status_code=503))

    try:
        stderr.write_text("service not ready\n")
        with pytest.raises(DaemonError) as caught:
            daemon.start()

        message = str(caught.value)
        assert "all ping attempts have failed" in message
        assert daemon.stderr_file_name in message
        assert "Stderr tail:\n\nservice not ready" in message
    finally:
        daemon._Daemon__files.close()


@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_context_failure_stops_process_and_preserves_error(tmp_path, error_type):
    daemon = Daemon(
        commands=[[common.python_path(), "-c", "import time; time.sleep(60)"]],
        cwd=str(tmp_path))
    error = error_type("original error")
    try:
        with pytest.raises(error_type) as caught:
            with daemon:
                daemon.start()
                raise error
        assert caught.value is error
        assert not daemon.is_alive()
        assert daemon._Daemon__stdout_file.closed
        assert daemon._Daemon__stderr_file.closed
    finally:
        process = daemon._Daemon__process
        if process is not None and process.process.poll() is None:
            process.process.kill()
        if process is not None:
            process.process.wait()


@pytest.mark.parametrize("context_error", [False, True])
def test_context_failure_does_not_wait_for_stuck_process(tmp_path, caplog, monkeypatch, context_error):
    clock = [0.0]
    signals = []
    child = mock.Mock()
    child.pid = 123
    child.poll.return_value = None
    child.terminate.side_effect = lambda: signals.append("TERM")
    child.kill.side_effect = lambda: signals.append("KILL")
    execution = mock.Mock()
    execution.process = child
    daemon = Daemon(commands=[["unused"]], cwd=str(tmp_path), cleanup_timeout=1)

    def sleep(seconds):
        clock[0] += seconds

    monkeypatch.setattr(daemon_module, "time", SimpleNamespace(
        monotonic=lambda: clock[0], sleep=sleep))
    daemon._Daemon__process = execution
    expected_error = RuntimeError if context_error else DaemonError
    expected_message = "original error" if context_error else "still running after SIGKILL"
    with pytest.raises(expected_error, match=expected_message):
        with daemon:
            if context_error:
                raise RuntimeError("original error")

    assert signals == ["TERM", "KILL"]
    assert clock[0] == pytest.approx(2)
    execution.wait.assert_not_called()
    assert daemon._Daemon__process is execution
    if context_error:
        assert "still running after SIGKILL" in caplog.text


@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_cleanup_diagnostics_do_not_replace_original_error(tmp_path, error_type):
    daemon = Daemon(commands=[["unused"]], cwd=str(tmp_path))
    execution = mock.Mock()
    execution.process.poll.return_value = 1
    execution.wait.side_effect = ValueError("diagnostics failed")
    daemon._Daemon__process = execution
    original_error = error_type("original error")

    with pytest.raises(error_type) as caught:
        with daemon:
            raise original_error

    assert caught.value is original_error
    assert daemon._Daemon__stdout_file.closed
    execution.wait.assert_called_once_with(check_exit_code=False)


def test_cleanup_skips_repeated_diagnostics(tmp_path):
    daemon = Daemon(commands=[["unused"]], cwd=str(tmp_path))
    execution = mock.Mock()
    execution.process.poll.return_value = 1
    daemon._Daemon__process = execution
    daemon._Daemon__diagnostics_checked = True

    with pytest.raises(RuntimeError, match="original error"):
        with daemon:
            raise RuntimeError("original error")

    execution.wait.assert_not_called()


def test_failure_includes_stderr_tail_and_exit_code(tmp_path):
    stdout = tmp_path / "stdout"
    stderr = tmp_path / "stderr"
    daemon = Daemon(
        commands=[["unused"]], cwd=str(tmp_path),
        stdout_file=str(stdout), stderr_file=str(stderr),
        stderr_on_error_lines=100, stderr_tail=True)
    daemon._Daemon__process = mock.Mock(returncode=1)
    try:
        stderr.write_text("old\n" * 150 + "last error\n")
        error = daemon.failure("mount failed")
        assert "Process exit_code = 1" in str(error)
        assert str(stdout) in str(error)
        assert str(stderr) in str(error)
        assert "last error" in str(error)
        assert str(error).splitlines().count("old") == 99
    finally:
        daemon._Daemon__files.close()


@pytest.mark.parametrize("stderr_tail", [False, True])
def test_failure_handles_missing_stderr(tmp_path, stderr_tail):
    stderr = tmp_path / "stderr"
    daemon = Daemon(
        commands=[["unused"]], cwd=str(tmp_path), stderr_file=str(stderr),
        stderr_on_error_lines=10, stderr_tail=stderr_tail)
    daemon._Daemon__process = mock.Mock(returncode=1)
    try:
        stderr.unlink()
        error = daemon.failure("client exited")
        assert "client exited" in str(error)
        assert "Process exit_code = 1" in str(error)
        assert str(stderr) in str(error)
        assert "unavailable" in str(error)
    finally:
        daemon._Daemon__files.close()


def test_failure_uses_head_of_stderr_by_default(tmp_path):
    stderr = tmp_path / "stderr"
    daemon = Daemon(
        commands=[["unused"]], cwd=str(tmp_path), stderr_file=str(stderr),
        stderr_on_error_lines=4)
    try:
        stderr.write_text("first\nsecond\nlast\n")
        error = daemon.failure("failed")
        assert "Stderr content:\n\nfirst\nsecond\nlast" in str(error)
        assert str(error).splitlines().count("last") == 1
    finally:
        daemon._Daemon__files.close()


@pytest.mark.parametrize("stderr_tail", [False, True])
@pytest.mark.parametrize("max_stderr_lines", [-1, 0, 1, 2, 3])
def test_failure_limits_stderr_log_lines(tmp_path, stderr_tail, max_stderr_lines):
    stderr = tmp_path / "stderr"
    daemon = Daemon(
        commands=[["unused"]], cwd=str(tmp_path), stderr_file=str(stderr),
        stderr_on_error_lines=max_stderr_lines, stderr_tail=stderr_tail)
    try:
        stderr.write_text("first\nsecond\nlast\n")
        error = daemon.failure("failed")
        label = "Stderr tail:" if stderr_tail else "Stderr content:"
        if max_stderr_lines <= 0:
            assert label not in str(error)
        else:
            details = str(error).split(label + "\n\n", 1)[1].splitlines()
            expected = ["first", "second", "last"]
            if stderr_tail:
                expected = expected[-max_stderr_lines:]
            else:
                expected = expected[:max_stderr_lines]
            assert details == expected
    finally:
        daemon._Daemon__files.close()


def test_constructor_closes_opened_files_on_failure(tmp_path):
    stdout = tmp_path / "stdout"
    streams = []
    original_open = open

    def open_log(*args, **kwargs):
        stream = original_open(*args, **kwargs)
        streams.append(stream)
        return stream

    with mock.patch.object(daemon_module, "open", side_effect=open_log, create=True):
        with pytest.raises(IsADirectoryError):
            Daemon(
                commands=[["unused"]], cwd=str(tmp_path),
                stdout_file=str(stdout), stderr_file=str(tmp_path))
    assert stdout.exists()
    assert len(streams) == 1
    assert streams[0].closed
