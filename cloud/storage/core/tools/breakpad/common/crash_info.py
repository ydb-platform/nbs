# -*- coding: UTF-8 -*-

import json
import logging
import os
import os.path
from uuid import uuid1

logger = logging.getLogger(__name__)


class CrashInfoStorageError(Exception):
    pass


class CrashInfo(object):

    def __init__(self):
        super(CrashInfo, self).__init__()
        self.corefile = None
        self.is_minidump = False
        self.time = None
        self.logfile = None
        self.info = None
        self.service = None
        self.metadata = dict()  # Additional metadata dictionary

    def dump(self):
        return json.dumps(dict(
            corefile=self.corefile,
            is_minidump=self.is_minidump,
            time=self.time,
            logfile=self.logfile,
            info=self.info,
            service=self.service,
            metadata=self.metadata,
        ))

    def load(self, data):
        obj = json.loads(data)
        self.corefile = obj.get("corefile")
        self.is_minidump = obj.get("is_minidump")
        self.time = obj.get("time")
        self.logfile = obj.get("logfile")
        self.info = obj.get("info")
        self.service = obj.get("service")
        self.metadata = obj.get("metadata", dict())


class CrashInfoStorage(object):
    FILETYPE = ".json"

    def __init__(self, queue_dir):
        super(CrashInfoStorage, self).__init__()
        self._logger = logger.getChild(self.__class__.__name__)
        self._queue_dir = queue_dir
        self._ensure_queuedir()

    def _ensure_queuedir(self):
        try:
            if not os.path.exists(self._queue_dir):
                os.makedirs(self._queue_dir, 0o700)
        except IOError as e:
            self._logger.error("Can't create directory %s %r", self._queue_dir, e)
            self._logger.debug("Exception", exc_info=True)
            raise CrashInfoStorageError("Error create queue directory")

    def put(self, crash_info):
        uuid = str(uuid1())
        filename = os.path.join(self._queue_dir, "{uuid}{filetype}".format(uuid=uuid, filetype=self.FILETYPE))
        self._logger.info("Save crash info to %s", filename)
        try:
            with open(filename, "w+") as fd:
                fd.write(crash_info.dump())
        except IOError as e:
            self._logger.error("Can't write file %s %r", filename, e)
            self._logger.debug("Exception", exc_info=True)
            raise CrashInfoStorageError("Error write crash info")

    def get(self):
        try:
            for name in os.listdir(self._queue_dir):
                if not name.endswith(self.FILETYPE):
                    continue
                filename = os.path.join(self._queue_dir, name)
                if not os.path.isfile(filename):
                    continue
                self._logger.info("Found crash info %s", filename)
                with open(filename, "r") as fd:
                    crash_info = CrashInfo()
                    crash_info.load(fd.read())
                os.unlink(filename)
                return crash_info
        except IOError as e:
            self._logger.error("Can't get crash info %r", e)
            self._logger.debug("Exception", exc_info=True)
            raise CrashInfoStorageError("Error read crash info")

        return None  # Not found
