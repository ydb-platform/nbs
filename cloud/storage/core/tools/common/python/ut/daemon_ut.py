import logging
import subprocess
from io import StringIO
from unittest import mock

import pytest
import yatest.common as common

import cloud.storage.core.tools.common.python.daemon as daemon_module
from cloud.storage.core.tools.common.python.daemon import Daemon


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
