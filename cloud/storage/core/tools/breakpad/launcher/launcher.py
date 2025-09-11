# -*- coding: UTF-8 -*-

import argparse
import datetime
import errno
import fcntl
import json
import logging
import os
import resource
import select
import shutil
import signal
import subprocess
import sys

from ..common.crash_info import CrashInfo, CrashInfoStorage

from .core_checker import CoreChecker
from .error_collector import StreamErrorCollector, FileErrorCollector
from .oom_checker import OOMChecker

logger = logging.getLogger(__name__)


class LauncherError(Exception):
    pass


class Executer:

    def __init__(self, command):
        self._logger = logger.getChild(self.__class__.__name__)
        self.command = command
        self.environ = dict()
        self.proc = None
        self.pid = -1
        self.err_out = StreamErrorCollector()
        self.err_err = StreamErrorCollector()
        self.set_signals()
        self.stop_time = None
        self.killed = False
        self.kill_timeout = 60

    def set_signals(self):
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)
        signal.signal(signal.SIGHUP, self.on_signal)

    def on_signal(self, signum, _):
        self._logger.info("signal %s received", signum)
        if self.proc is None:
            return
        self.proc.send_signal(signum)
        if self.stop_time is None and (signum == signal.SIGTERM or signum == signal.SIGINT):
            self.stop_time = datetime.datetime.now()

    def set_env(self, key, value):
        self.environ[key] = value

    @staticmethod
    def set_nonblock(stream):
        flags = fcntl.fcntl(stream.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(stream.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)

    def nb_read(self, stream):
        try:
            return stream.read()
        except OSError as e:
            # Read can return: "OSError: [Errno 11] Resource temporarily unavailable"
            if e.errno != errno.EAGAIN:
                self._logger.debug(
                    "Error occured during non-blocking read %r", e)
        return ""

    def tee_stream(self, stream):
        data = self.nb_read(stream)
        if len(data) == 0:
            return
        if stream == self.proc.stdout:
            sys.stdout.write(data)
            self.err_out.write(data)
        elif stream == self.proc.stderr:
            sys.stderr.write(data)
            self.err_err.write(data)

    def run(self):
        self._logger.debug("Execute: %r", self.command)
        env = os.environ.copy()
        env.update(self.environ)
        try:
            self.proc = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                shell=False,
                close_fds=True
            )
        except Exception as e:
            self._logger.error("Execution of '%r' failed %r", self.command, e)
            self._logger.debug("Exception", exc_info=True)
            return -1
        self.pid = self.proc.pid

        streams = [self.proc.stdout, self.proc.stderr]
        fd2stream = {fd.fileno(): fd for fd in streams}
        poller = select.poll()
        for fd in streams:
            poller.register(fd, select.POLLERR | select.POLLIN | select.POLLNVAL)
            Executer.set_nonblock(fd)

        while self.proc.poll() is None and len(streams) > 0:
            try:
                fds = poller.poll(500)
            except select.error as e:
                if e[0] == errno.EINTR:
                    continue
                raise

            if self.stop_time is not None and not self.killed and \
               (datetime.datetime.now() - self.stop_time).total_seconds() > self.kill_timeout:
                self._logger.error("Terminate timeout, kill")
                self.killed = True
                self.proc.kill()

            if len(fds) == 0:
                continue
            for fd, ev in fds:
                if (ev & select.POLLIN) != 0:
                    self.tee_stream(fd2stream[fd])
                else:
                    poller.unregister(fd)
                    stream = fd2stream[fd]
                    if stream in streams:
                        streams.remove(stream)
        self.proc.wait()

        for fd in (self.proc.stdout, self.proc.stderr):
            self.tee_stream(fd)

        return self.proc.returncode

    def get_errors(self):
        return self.err_err.errors + self.err_out.errors


class Launcher:

    def __init__(self):
        self._logger = logger.getChild(self.__class__.__name__)
        self.launcher = None
        self.errors = ""
        self.storage = None
        self.storage_coredir = None
        self.storage_logdir = None
        self.logfile = None
        self.args = None

    def parse_args(self):
        parser = argparse.ArgumentParser(
            description="Breakpad Executer",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument("-l", "--log", type=str, metavar="PATH",
                            help="Search VERIFY/FAIL messages in this logfile")
        parser.add_argument("--enable-rotate-log", action="store_true")
        parser.add_argument("--breakpad-enable", action="store_true",
                            help="Use Breakpad")
        # TODO: use more generic default
        parser.add_argument("--breakpad-lib", type=str, metavar="PATH",
                            default="libbreakpad_init_nbs.so",
                            help="Path to breakpad library")
        parser.add_argument("--breakpad-coredir", type=str, metavar="DIR",
                            default="/var/tmp/breakpad/coredir",
                            help="Directory to search for Breakpad minidumps")
        parser.add_argument("--coredir", type=str, metavar="DIR",
                            default="/coredumps",
                            help="Directory to search for coredumps")
        parser.add_argument("--datadir", type=str, metavar="DIR",
                            default="/var/tmp/breakpad",
                            help="Directory to store queued crashes"
                                 "for breakpad-sender")
        parser.add_argument("--verbose", action="store_true")
        parser.add_argument("--enable-collect-oom", action="store_true")
        parser.add_argument("--service",  type=str,
                            help="Service name (will be deduced from binary "
                                 "if not set)")
        parser.add_argument("--metadata", type=json.loads,
                            help="Additional metadata, JSON format")

        parser.add_argument("cmd", metavar="CMD")
        parser.add_argument("args", metavar="ARGS", nargs="*")
        self.args = parser.parse_args()

    @property
    def breakpad_coredir(self):
        pid = os.getpid()
        return os.path.join(self.args.breakpad_coredir, str(pid))

    @property
    def service(self):
        if self.args.service:
            return self.args.service
        if self.args.cmd:
            return os.path.basename(self.args.cmd)
        return None

    def ensure_directory(self, path):
        try:
            os.makedirs(path, 0o700, exist_ok=True)
        except OSError as e:
            self._logger.error("Can't create directory %s %r", path, e)
            self._logger.debug("Exception", exc_info=True)
            raise LauncherError("Error create directory")

    def init_breakpad_coredir(self):
        self.clean_breakpad_coredir()
        self.ensure_directory(self.breakpad_coredir)

    def clean_breakpad_coredir(self):
        try:
            shutil.rmtree(self.breakpad_coredir, ignore_errors=True)
        except IOError as e:
            self._logger.error("Can't remove directory %s %r", self.breakpad_coredir, e)
            self._logger.debug("Exception", exc_info=True)
            raise LauncherError("Error remove directory")

    @staticmethod
    def disable_buffering():
        sys.stdout = os.fdopen(sys.stdout.fileno(), "w", 0)
        sys.stderr = os.fdopen(sys.stderr.fileno(), "w", 0)

    def collect_errors(self):
        errors = self.launcher.get_errors()
        if self.args.log:
            try:
                log = FileErrorCollector(self.logfile)
            except OSError as e:
                self._logger.warning("Error read log file %s %r", self.logfile, e)
            else:
                errors = errors + log.errors

        if len(errors) > 0:
            self.errors = "\n".join(errors[-1])
        else:
            self.errors = ""

    def core_time(self, corefile):
        try:
            return int(os.path.getmtime(corefile))
        except OSError as e:
            self._logger.error("Can't get mtime %s %r", corefile, e)
            self._logger.debug("Exception", exc_info=True)
            raise LauncherError("Can't get core mtime")

    def collect_minidump(self):
        for corefile in CoreChecker().cores(self.breakpad_coredir, pid=None):
            logging.info("Found minidump %s", corefile)
            info = CrashInfo()
            info.time = self.core_time(corefile)
            info.corefile = self.save_minidump(corefile)
            info.is_minidump = True
            info.logfile = self.logfile
            info.info = self.errors
            info.service = self.service
            info.metadata = self.args.metadata
            self.storage.put(info)

    def collect_coredump(self):
        for corefile in CoreChecker().cores(self.args.coredir, pid=self.launcher.pid):
            logging.info("Found coredump %s", corefile)
            info = CrashInfo()
            info.time = self.core_time(corefile)
            info.corefile = corefile
            info.is_minidump = False
            info.logfile = self.logfile
            info.info = self.errors
            info.service = self.service
            info.metadata = self.args.metadata
            self.storage.put(info)

    def collect_oom(self):
        oom_checker = OOMChecker()
        if not oom_checker.check(self.launcher.pid):
            logging.info("OOM not found")
            return
        logging.info("Found OOM")
        info = CrashInfo()
        info.time = oom_checker.time
        info.logfile = self.args.log
        info.info = self.errors + "\n" + oom_checker.text
        info.service = oom_checker.service
        info.metadata = self.args.metadata
        self.storage.put(info)

    def rotate_logfile(self):
        name = os.path.basename(self.logfile) + datetime.datetime.now().strftime(".%Y-%m-%d-%H-%M-%S")
        dst = os.path.join(self.storage_logdir, name)
        self._logger.info("Rotate log to %s", dst)
        try:
            shutil.copy(self.logfile, dst)
            self.logfile = dst
        except IOError as e:
            self._logger.error("Can't copy file %s to %s %r", self.logfile, dst, e)
            self._logger.debug("Exception", exc_info=True)

    def save_minidump(self, filename):
        dst = os.path.join(self.storage_coredir, os.path.basename(filename))
        self._logger.info("Save minidump to %s", dst)
        try:
            shutil.move(filename, dst)
        except IOError as e:
            self._logger.error("Can't save minidump to %s %r", dst, e)
            self._logger.debug("exception", exc_info=True)
            raise LauncherError("Error save minidump")
        return dst

    def launch(self):
        return_code = self.launcher.run()
        if return_code == 0:
            return return_code
        self._logger.info("Return code: %d", return_code)
        self.logfile = self.args.log
        if self.args.enable_rotate_log:
            self.rotate_logfile()
        self.collect_errors()
        if self.args.breakpad_enable:
            self.collect_minidump()
        else:
            self.collect_coredump()
        if self.args.enable_collect_oom:
            self.collect_oom()
        return return_code

    def init(self):
        self.storage_logdir = os.path.join(self.args.datadir, "logs")
        self.ensure_directory(self.storage_logdir)
        self.storage_coredir = os.path.join(self.args.datadir, "cores")
        self.ensure_directory(self.storage_coredir)
        self.storage = CrashInfoStorage(os.path.join(self.args.datadir, "queue"))
        self.disable_buffering()
        self.launcher = Executer([self.args.cmd] + self.args.args)

    def run(self):
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
        self.parse_args()
        if self.args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        try:
            self.init()
            if self.args.breakpad_enable:
                resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
                self.init_breakpad_coredir()
                self.launcher.set_env("LD_PRELOAD", self.args.breakpad_lib)
                self.launcher.set_env("BREAKPAD_MINIDUMPS_PATH", self.breakpad_coredir)
            return self.launch()
        except Exception as e:
            self._logger.error("%r", e)
            self._logger.debug("Exception", exc_info=True)
            return 1
        finally:
            if self.args.breakpad_enable:
                self.clean_breakpad_coredir()


def main():
    sys.exit(Launcher().run())
