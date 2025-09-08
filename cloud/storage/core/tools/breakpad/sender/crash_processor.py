# -*- coding: UTF-8 -*-

from socket import getfqdn

from ..common.crash_info import CrashInfo, CrashInfoProcessed
from .conductor import Conductor
from .coredump import Coredump, Minidump
from .coredump_formatter import CoredumpFormatter


class CrashProcessorError(Exception):
    pass


class CrashProcessor:
    def process(self, crash: CrashInfo) -> CrashInfoProcessed:
        pass

    def cleanup():
        pass


class OOMCrashProcessor(CrashProcessor):
    def process(self, crash: CrashInfo) -> CrashInfoProcessed:
        processed = CrashInfoProcessed(crash)
        processed.crash_type = CrashInfoProcessed.CRASH_TYPE_OOM
        processed.backtrace = "Thread 1 (LWP 0):\n#0  0x0000000000000000 in main () at main.cc:0\n",
        return processed


class CoredumpCrashProcessor(CrashProcessor):
    def __init__(self, gdb_timeout):
        self._gdb_timeout = gdb_timeout
        self._core = None

    def _get_server(self, crash: CrashInfo):
        if "server" in crash.metadata:
            return crash.metadata["server"]

        return getfqdn()

    def _get_cluster(self, crash: CrashInfo):
        if "cluster" in crash.metadata:
            return crash.metadata["cluster"]

        try:
            return Conductor().primary_group
        except Exception:
            return "error"

    def process(self, crash: CrashInfo) -> CrashInfoProcessed:
        processed = CrashInfoProcessed(crash)
        processed.crash_type = CrashInfoProcessed.CRASH_TYPE_CORE

        try:
            corefile = crash.corefile
            self._core = Minidump(corefile, self._gdb_timeout) \
                if crash.is_minidump else Coredump(corefile, self._gdb_timeout)

            processed.backtrace = self._core.backtrace
            processed.service = self._core.service

            formatter = CoredumpFormatter(processed.backtrace)
            processed.formatted_backtrace = formatter.format()
        except Exception as e:
            raise CrashProcessorError(e)

        processed.server = self._get_server(crash) or "unknown"
        processed.cluster = self._get_cluster(crash) or "unknown"

        return processed

    def cleanup(self):
        if self._core:
            self._core.cleanup()
