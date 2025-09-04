import datetime
import logging
import os
import requests
import subprocess
import tempfile
import threading
import time

import yatest.common as common


logger = logging.getLogger(__name__)


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

    def __init__(self, message, exit_code=None):
        message = "Daemon failed with message: {}.".format(message)
        if exit_code is not None:
            message += " Process exit_code = {}.".format(exit_code)
        super(DaemonError, self).__init__(message)


class Daemon(object):

    def __init__(self, commands, cwd, restart_allowed=False,
                 restart_interval=None, ping_port=None, ping_path=None,
                 ping_success_codes=None, ping_timeout=2,
                 ping_attempts=0, core_pattern=None, service_name=None):

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

        stdin_prefix = "stdin_"
        stdout_prefix = "stdout_"
        stderr_prefix = "stderr_"
        if service_name is not None:
            stdin_prefix = "{}_{}".format(service_name, stdin_prefix)
            stdout_prefix = "{}_{}".format(service_name, stdout_prefix)
            stderr_prefix = "{}_{}".format(service_name, stderr_prefix)

        self.__stdin_file = tempfile.NamedTemporaryFile(dir=self.__cwd, prefix=stdin_prefix, delete=False)
        self.__stdout_file = tempfile.NamedTemporaryFile(dir=self.__cwd, prefix=stdout_prefix, delete=False)
        self.__stderr_file = tempfile.NamedTemporaryFile(dir=self.__cwd, prefix=stderr_prefix, delete=False)

        self.__lock = threading.Lock()
        self.__process = None
        self.__timer = None
        self.__step = 0

    # Should be guarded by self.__lock.
    def __poll(self):
        return self.__process.process.poll()

    # Should be guarded by self.__lock.
    def __verify_process(self):
        res = self.__poll()
        if res is not None:
            logger.error("unexpected exit code {}".format(res))
            self.__process.verify_no_coredumps()
            self.__process.verify_sanitize_errors()
            raise DaemonError("unexpected exit code", res)

    # Should be guarded by self.__lock.
    def __start_process(self, command):
        logger.info("starting process {}".format(command))
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
        self.__process.kill()
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
                    raise DaemonError("unexpected exit code", res)

            time.sleep(self.__ping_timeout)
            attempts += 1

        if not success:
            logger.fatal("all ping attempts have failed")
            raise DaemonError("all ping attempts have failed")

        self.__step += 1
        start_ts = datetime.datetime.now()
        logger.info("subprocess started at {}".format(start_ts))

    # Should be guarded by self.__lock.
    def __schedule_restart(self):
        if self.__restart_allowed:
            self.__timer = threading.Timer(self.__restart_interval, self.__restart)
            self.__timer.start()

    def __restart(self):
        with self.__lock:
            if self.__process is not None:
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
                logger.warn("process is empty")
                return False

            if not self.__process.running:
                logger.warn("process (pid={}) is not running".format(
                    self.__process.process.pid))
                return False
        return True
