import datetime
import contextlib
import logging
import os
import requests
import subprocess
import tempfile
import threading
import time

import yatest.common as common


logger = logging.getLogger(__name__)

_MAX_LOG_TAIL_BYTES = 64 * 1024


def extract_log(log_file, max_lines=0, label="Stderr", *, tail=False):
    """Extract bounded log content with a label and optional truncation marker.

    Args:
        log_file: Path to the log file; None disables extraction.
        max_lines: Maximum number of log lines; nonpositive values disable extraction.
        label: Name used in the header and read error messages.
        tail: Read the last 64 KiB and lines instead of the first.
    """
    if max_lines <= 0 or log_file is None:
        return []

    try:
        with open(log_file, 'rb') as log:
            file_size = log.seek(0, os.SEEK_END)
            start = max(0, file_size - _MAX_LOG_TAIL_BYTES) if tail else 0
            log.seek(start)
            data = log.read(_MAX_LOG_TAIL_BYTES)
    except OSError as error:
        return ["{}{} unavailable: {}".format(label, " tail" if tail else "", error)]

    lines = data.splitlines()
    selected = lines[-max_lines:] if tail else lines[:max_lines]
    content = [line.decode('utf-8', errors='replace') for line in selected]
    truncated = start > 0 if tail else file_size > len(data)
    if truncated and len(lines) <= max_lines:
        marker = "[truncated to the {} {} KiB; {} line may be incomplete]".format(
            "last" if tail else "first", _MAX_LOG_TAIL_BYTES // 1024,
            "first" if tail else "last")
        content.insert(0 if tail else len(content), marker)
    return ["{} {}:".format(label, "tail" if tail else "content"), ""] + content


def extract_log_tail(log_file, max_lines=0, label="Stderr"):
    """Return at most max_lines log lines from the last 64 KiB of a file."""
    return extract_log(log_file, max_lines, label, tail=True)


def extract_log_head(log_file, max_lines=0, label="Stderr"):
    """Return at most max_lines log lines from the first 64 KiB of a file."""
    return extract_log(log_file, max_lines, label, tail=False)


def _log_process_threads(pid):
    task_dir = "/proc/{}/task".format(pid)
    try:
        thread_ids = sorted(
            (entry for entry in os.listdir(task_dir) if entry.isdigit()),
            key=int)
    except OSError as error:
        logger.warning("failed to list threads for PID %s: %s", pid, error)
        return

    logger.warning("PID %s has %s thread(s)", pid, len(thread_ids))
    for thread_id in thread_ids:
        diagnostics = []
        # These per-thread procfs files help explain why a process remains
        # unreapable after SIGKILL and wait:
        #   status - thread state (notably D for uninterruptible sleep and Z
        #            for zombie) and other thread metadata.
        #            Does not require elevated permissions.
        #            https://man7.org/linux/man-pages/man5/proc_pid_status.5.html
        #   wchan  - kernel function in which a sleeping thread is blocked.
        #            Does not require elevated permissions if process has same UID.
        #            https://man7.org/linux/man-pages/man5/proc_pid_wchan.5.html
        #   stack  - kernel call stack, which can identify the blocked I/O or
        #            subsystem; reading it most definetely require elevated permissions.
        #            https://man7.org/linux/man-pages/man5/proc_pid_stack.5.html
        # Thread directories are documented at:
        # https://man7.org/linux/man-pages/man5/proc_pid_task.5.html
        for name in ("status", "wchan", "stack"):
            path = os.path.join(task_dir, thread_id, name)
            try:
                with open(path, errors="replace") as proc_file:
                    contents = proc_file.read().rstrip()
            except OSError as error:
                contents = "<failed to read {}: {}>".format(path, error)
            diagnostics.append("{}:\n{}".format(name, contents))

        logger.warning(
            "PID %s TID %s diagnostics:\n%s",
            pid,
            thread_id,
            "\n".join(diagnostics))


def _on_wait_timeout(ex, timeout):
    logger.warning(
        f"wait for pid {ex.process.pid} timed out after {timeout} seconds"
    )

    bt = subprocess.getoutput(
        f'sudo gdb --batch -p {ex.process.pid} -ex "thread apply all bt"'
    )
    logger.warning(f"PID {ex.process.pid}: backtrace:\n{bt}")


def _wait_process(ex):
    while True:
        try:
            return ex.wait(
                check_exit_code=False,
                timeout=60,
                on_timeout=_on_wait_timeout)
        except common.ExecutionTimeoutError:
            pass


class DaemonError(RuntimeError):

    def __init__(self, message, exit_code=None, stdout=None, stderr=None,
                 max_stderr_lines=0, stderr_tail=False):
        """Describe a daemon failure with optional process and log details.

        Args:
            message: Reason for the failure.
            exit_code: Process exit status, if known.
            stdout: Path to the stdout log, if available.
            stderr: Path to the stderr log, if available.
            max_stderr_lines: Maximum number of stderr log lines; zero omits them.
            stderr_tail: Include the last stderr lines instead of the first.
        """
        message = "Daemon failed with message: {}.".format(message)
        if exit_code is not None:
            message += " Process exit_code = {}.".format(exit_code)
        if stdout is not None:
            message += "\nStdout file name: \n{}".format(stdout)
        if stderr is not None:
            message += "\nStderr file name: \n{}".format(stderr)
        if max_stderr_lines > 0 and stderr is not None:
            details = extract_log(stderr, max_stderr_lines, tail=stderr_tail)
            if details:
                message += "\n" + "\n".join(details)
        super(DaemonError, self).__init__(message)


class Daemon(object):

    def __init__(self, commands, cwd, restart_allowed=False,
                 restart_interval=None, ping_port=None, ping_path=None,
                 ping_success_codes=None, ping_timeout=2,
                 ping_attempts=0, core_pattern=None, service_name=None,
                 stdout_file=None, stderr_file=None,
                 stderr_on_error_lines=0, stderr_tail=False,
                 cleanup_timeout=10):

        self.__commands = commands
        self.__cwd = cwd
        self.__restart_allowed = restart_allowed
        self.__restart_interval = restart_interval
        self.__ping_port = ping_port
        self.__ping_path = ping_path
        self.__ping_success_codes = ping_success_codes
        self.__ping_timeout = ping_timeout
        self.__ping_attempts = ping_attempts
        self.__core_pattern = core_pattern
        self.__stderr_on_error_lines = stderr_on_error_lines
        self.__stderr_tail = stderr_tail
        self.__cleanup_timeout = cleanup_timeout

        stdin_prefix = "stdin_"
        stdout_prefix = "stdout_"
        stderr_prefix = "stderr_"
        if service_name is not None:
            stdin_prefix = "{}_{}".format(service_name, stdin_prefix)
            stdout_prefix = "{}_{}".format(service_name, stdout_prefix)
            stderr_prefix = "{}_{}".format(service_name, stderr_prefix)

        with contextlib.ExitStack() as files:
            self.__stdin_file = files.enter_context(tempfile.NamedTemporaryFile(
                dir=self.__cwd, prefix=stdin_prefix, delete=False))
            self.__stdout_file = files.enter_context(
                open(stdout_file, 'wb') if stdout_file is not None else
                tempfile.NamedTemporaryFile(dir=self.__cwd, prefix=stdout_prefix, delete=False))
            if (stdout_file is not None and stderr_file is not None
                    and os.path.abspath(stdout_file) == os.path.abspath(stderr_file)):
                self.__stderr_file = self.__stdout_file
            else:
                self.__stderr_file = files.enter_context(
                    open(stderr_file, 'wb') if stderr_file is not None else
                    tempfile.NamedTemporaryFile(dir=self.__cwd, prefix=stderr_prefix, delete=False))
            self.__files = files.pop_all()

        self.__lock = threading.Lock()
        self.__process = None
        self.__timer = None
        self.__step = 0
        # Avoid repeating core and sanitizer checks during cleanup after __verify_process.
        self.__diagnostics_checked = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if exc_type is not None:
                try:
                    self.__cleanup_after_failure()
                except Exception:
                    logger.exception("Daemon cleanup or diagnostics failed")
                if not isinstance(exc_value, DaemonError):
                    try:
                        logger.error("%s", self.failure(str(exc_value)))
                    except Exception:
                        logger.exception("Could not read daemon failure details")
        finally:
            self.__files.close()
        return False

    def __cleanup_after_failure(self):
        with self.__lock:
            if self.__timer is not None:
                self.__timer.cancel()
                self.__timer = None
            execution = self.__process
            if execution is None:
                return

            child = execution.process
            for send_signal in (child.terminate, child.kill):
                if child.poll() is not None:
                    break
                try:
                    send_signal()
                except OSError:
                    if child.poll() is None:
                        logger.exception("Could not signal daemon pid %s", child.pid)
                deadline = time.monotonic() + self.__cleanup_timeout
                while child.poll() is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    time.sleep(min(0.1, remaining))

            if child.poll() is None:
                logger.error("Daemon pid %s is still running after SIGKILL; skipping wait", child.pid)
            elif not self.__diagnostics_checked:
                self.__diagnostics_checked = True
                execution.wait(check_exit_code=False)

    def failure(self, message):
        with self.__lock:
            return self.__make_error(message)

    # Should be guarded by self.__lock.
    def __make_error(self, message, exit_code=None):
        if exit_code is None and self.__process is not None:
            exit_code = self.__process.returncode
        return DaemonError(
            message,
            exit_code=exit_code,
            stdout=self.stdout_file_name,
            stderr=self.stderr_file_name,
            max_stderr_lines=self.__stderr_on_error_lines,
            stderr_tail=self.__stderr_tail)

    # Should be guarded by self.__lock.
    def __poll(self):
        return self.__process.process.poll()

    # Should be guarded by self.__lock.
    def __verify_process(self):
        res = self.__poll()
        if res is not None:
            logger.error("unexpected exit code {}".format(res))
            self.__diagnostics_checked = True
            self.__process.verify_no_coredumps()
            self.__process.verify_sanitize_errors()
            raise self.__make_error("unexpected exit code", res)

    # Should be guarded by self.__lock.
    def __start_process(self, command):
        logger.info("starting process {}".format(command))
        self.__diagnostics_checked = False
        self.__process = common.execute(
            command=command,
            cwd=self.__cwd,
            stdin=self.__stdin_file,
            stdout=self.__stdout_file,
            stderr=self.__stderr_file,
            wait=False,
            core_pattern=self.__core_pattern
        )
        self.__verify_process()

    # Should be guarded by self.__lock.
    def __terminate_process(self):
        self.__verify_process()
        logger.info("terminating process")
        self.__process.terminate()
        _wait_process(self.__process)
        self.__process = None

    # Should be guarded by self.__lock.
    def __kill_process(self):
        self.__verify_process()
        logger.info("killing process")
        try:
            self.__process.kill()
        except common.TimeoutError as error:
            try:
                self.__process.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _log_process_threads(self.__process.process.pid)
                raise error
        self.__process.wait(check_exit_code=False)
        self.__process = None

    # Should be guarded by self.__lock.
    def __ping(self):
        try:
            if self.__ping_port is None:
                return True

            endpoint = "http://localhost:{}{}".format(self.__ping_port, self.__ping_path)
            logger.debug("ping {} ...".format(endpoint))
            r = requests.get(endpoint)
            if not self.__ping_success_codes:
                return True

            if r.status_code in self.__ping_success_codes:
                return True

            logger.info("ping attempt has failed. Bad status code: {}".format(r.status_code))
        except Exception as e:
            logger.info("ping attempt has failed: {}".format(e))

        return False

    # Should be guarded by self.__lock.
    def __start(self):
        command = self.__commands[self.__step % len(self.__commands)]
        self.__start_process(command)

        attempts = 0
        success = False
        while attempts < self.__ping_attempts or self.__ping_attempts == 0:
            if self.__ping():
                success = True
                break

            res = self.__poll()
            if res is not None:
                if res in [0, 1, 100]:
                    logger.info("subprocess failed to start, code {}".format(res))
                    self.__start_process(command)
                else:
                    logger.fatal("unexpected exit code {}".format(res))
                    self.__verify_process()

            time.sleep(self.__ping_timeout)
            attempts += 1

        if not success:
            logger.fatal("all ping attempts have failed")
            raise self.__make_error("all ping attempts have failed")

        self.__step += 1
        start_ts = datetime.datetime.now()
        logger.info("subprocess started at {}".format(start_ts))

    # Should be guarded by self.__lock.
    def __schedule_restart(self):
        if self.__restart_allowed:
            logger.info(f"schedule restart: {self.__restart_interval}s")
            self.__timer = threading.Timer(self.__restart_interval, self.__restart)
            self.__timer.start()

    def __restart(self):
        with self.__lock:
            if self.__process is not None:
                logger.info("restart the process")
                self.__kill_process()
                self.__start()
                self.__schedule_restart()

    def allow_restart(self):
        logger.info("allow restart")
        with self.__lock:
            self.__restart_allowed = True
            self.__schedule_restart()

    def start(self):
        with self.__lock:
            if self.__process is not None:
                return

            self.__start()
            self.__schedule_restart()

    def stop(self):
        with self.__lock:
            if self.__process is not None:
                if self.__timer is not None:
                    self.__timer.cancel()
                    self.__timer = None

                self.__terminate_process()

    def kill(self):
        with self.__lock:
            if self.__process is not None:
                if self.__timer is not None:
                    self.__timer.cancel()
                    self.__timer = None

                self.__kill_process()

    @property
    def command(self):
        with self.__lock:
            if self.__process:
                return self.__process.command
        return None

    @property
    def returncode(self):
        with self.__lock:
            if self.__process:
                return self.__process.returncode
        return None

    @property
    def pid(self):
        with self.__lock:
            return self.__process.process.pid

    @property
    def stdin_file_name(self):
        return os.path.abspath(self.__stdin_file.name)

    @property
    def stdout_file_name(self):
        return os.path.abspath(self.__stdout_file.name)

    @property
    def stderr_file_name(self):
        return os.path.abspath(self.__stderr_file.name)

    def is_alive(self):
        with self.__lock:
            if self.__process is None:
                logger.warning("process is empty")
                return False

            if not self.__process.running:
                logger.warning("process (pid={}) is not running".format(
                    self.__process.process.pid))
                return False
        return True
