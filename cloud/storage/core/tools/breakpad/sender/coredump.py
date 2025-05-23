# -*- coding: UTF-8 -*-

import logging
import os
import re
import subprocess

from .command import Command

logger = logging.getLogger(__name__)


class CoredumpError(Exception):
    pass


class MinidumpError(CoredumpError):
    pass


class Coredump(object):
    GDB_CMD = "/usr/bin/gdb"
    GDB_TIMEOUT = 300  # Seconds
    EXECFILE_RE = re.compile(r'^Core was generated by `(.+)\'\.$', re.M)

    def __init__(self, corefile):
        super(Coredump, self).__init__()
        self._logger = logger.getChild(self.__class__.__name__)
        self._corefile = corefile
        self._execfile = None

    def _command(self, cmd):
        try:
            cmd = Command(cmd, timeout=self.GDB_TIMEOUT)
            exit_code, out, _ = cmd.run()
        except Exception:
            raise CoredumpError("Error exec command")
        if exit_code != 0:
            self._logger.error("Command %r exit code %s", cmd, exit_code)
            raise CoredumpError("Command error exit code " + str(exit_code))
        return out

    @property
    def execfile(self):
        if self._execfile:
            return self._execfile
        cmd = [self.GDB_CMD, "--core", self._corefile, "--batch", "-q"]
        file_output = self._command(cmd)
        search_result = self.EXECFILE_RE.search(file_output)
        if search_result is None:
            self._logger.error("Execfile not found %s", file_output)
            raise CoredumpError("Execfile not found")
        self._execfile = search_result.group(1)
        path_with_args = self._execfile.split(' ')
        while len(path_with_args) > 0:
            expect_path = ' '.join(path_with_args)
            if os.path.isfile(expect_path):
                self._execfile = expect_path
                break
            path_with_args.pop()
        return self._execfile

    @property
    def service(self):
        return os.path.basename(self.execfile)

    @property
    def backtrace(self):
        cmd = [
            self.GDB_CMD,
            self.execfile,
            self._corefile,
            "-iex=set auto-load safe-path /",
            "-iex=set print thread-events off",
            "-ex=thread apply all bt",
            "--batch",
            "-q"
        ]
        return self._command(cmd)

    def cleanup(self):
        if not os.path.isfile(self._corefile):
            return
        try:
            os.unlink(self._corefile)
        except OSError as e:
            self._logger.warning("unlink %s failed %r", self._corefile, e)


class Minidump(Coredump):
    MINIDUMP2CORE_CMD = "/usr/bin/minidump-2-core"

    def __init__(self, minidump):
        super(Minidump, self).__init__(minidump + ".core")
        self._logger = logger.getChild(self.__class__.__name__)
        self._minidump = minidump
        self._make_corefile()

    def _make_corefile(self):
        cmd = (self.MINIDUMP2CORE_CMD, self._minidump)
        self._logger.debug("Exec %r", cmd)
        self._logger.info("Save core to %s", self._corefile)
        try:
            with open(self._corefile, "w+") as corefile:
                with open(os.devnull, "w") as devnull:
                    process = subprocess.Popen(
                        cmd,
                        stdout=corefile,
                        stderr=devnull,
                        close_fds=True,
                        shell=False
                    )
                    exit_code = process.wait()
        except IOError as e:
            self._logger.error("Error write corefile %s %r", self._corefile, e)
            raise MinidumpError("Error write corefile")
        if exit_code != 0:
            self._logger.error("Error convert minidump %d", exit_code)
            raise MinidumpError("Error convert minidump " + str(exit_code))

    def cleanup(self):
        super(Minidump, self).cleanup()
        if not os.path.isfile(self._minidump):
            return
        try:
            os.unlink(self._minidump)
        except OSError as e:
            self._logger.warning("unlink %s failed %r", self._minidump, e)
