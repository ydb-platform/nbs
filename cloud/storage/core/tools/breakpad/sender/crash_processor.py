# -*- coding: UTF-8 -*-

from abc import abstractmethod, ABC
from socket import getfqdn
from os import getenv

from ..common.crash_info import CrashInfo, CrashInfoProcessed
from .conductor import Conductor
from .coredump import Coredump, Minidump, CoredumpError, MinidumpError
from .coredump_formatter import CoredumpFormatter


class CrashProcessorError(Exception):
    pass


class CrashProcessor(ABC):
    @abstractmethod
    def process(self, crash: CrashInfo) -> CrashInfoProcessed:
        pass

    @abstractmethod
    def cleanup():
        pass


class OOMCrashProcessor(CrashProcessor):
    def process(self, crash: CrashInfo) -> CrashInfoProcessed:
        processed = CrashInfoProcessed(crash)
        processed.crash_type = CrashInfoProcessed.CRASH_TYPE_OOM
        processed.backtrace = "Thread 1 (LWP 0):\n#0  0x0000000000000000 in main () at main.cc:0\n",
        return processed

    def cleanup(self):
        pass


class CoredumpCrashProcessor(CrashProcessor):
    def __init__(self, gdb_timeout, gdb_disabled, conductor_enabled):
        self._gdb_timeout = gdb_timeout
        self._gdb_disabled = gdb_disabled
        self._conductor_enabled = conductor_enabled
        self._core = None

    def _get_server(self, crash: CrashInfo):
        from_metadata = crash.metadata.get("server")
        from_env = getenv("BREAKPAD_SERVER")

        return from_metadata or from_env or getfqdn() or "unknown"

    def _get_cluster(self, crash: CrashInfo):
        from_metadata = crash.metadata.get("cluster")
        from_env = getenv("BREAKPAD_CLUSTER")
        from_conductor = \
            Conductor().primary_group if self._conductor_enabled else None

        return from_metadata or from_env or from_conductor or "unknown"

    def process(self, crash: CrashInfo) -> CrashInfoProcessed:
        processed = CrashInfoProcessed(crash)
        processed.crash_type = CrashInfoProcessed.CRASH_TYPE_CORE

        try:
            corefile = crash.corefile
            if crash.is_minidump:
                self._core = Minidump(
                    corefile, self._gdb_timeout, self._gdb_disabled)
            else:
                self._core = Coredump(
                    corefile, self._gdb_timeout, self._gdb_disabled)

            processed.backtrace = self._core.backtrace
            processed.service = self._core.service

            formatter = CoredumpFormatter(processed.backtrace)
            processed.formatted_backtrace = formatter.format()
        except (CoredumpError, MinidumpError) as e:
            raise CrashProcessorError from e

        processed.server = self._get_server(crash)
        processed.cluster = self._get_cluster(crash)

        return processed

    def cleanup(self):
        if self._core:
            self._core.cleanup()
