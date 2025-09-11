# -*- coding: UTF-8 -*-

import logging
import os

logger = logging.getLogger(__name__)


class CoreChecker:

    def __init__(self):
        self._logger = logger.getChild(self.__class__.__name__)
        self._coredir = None
        self._pid = None

    def _minidumps(self):
        for filename in os.listdir(self._coredir):
            if not filename.endswith(".dmp"):
                continue
            fullpath = os.path.join(self._coredir, filename)
            yield fullpath

    def _cores(self):
        for filename in os.listdir(self._coredir):
            parts = filename.split('.')
            try:
                pid = int(parts[-2])
            except (IndexError, ValueError):
                continue
            if pid != self._pid:
                continue
            fullpath = os.path.join(self._coredir, filename)
            yield fullpath

    def cores(self, coredir, pid=None):
        self._coredir = coredir
        self._pid = pid
        try:
            for corefile in self._minidumps() if pid is None else self._cores():
                yield corefile
        except IOError as e:
            self._logger.error("Error check corefiles %r", e)
            self._logger.debug("Exception", exc_info=True)
