# -*- coding: UTF-8 -*-

import logging
import datetime

logger = logging.getLogger(__name__)


class Limiter:

    def __init__(self, window_seconds, limit):
        self._logger = logger.getChild(self.__class__.__name__)
        self.window_duration = datetime.timedelta(seconds=window_seconds)
        self.items_limit = limit
        self._items = list()

    def _cleanup_expired(self, now):
        expire_time = now - self.window_duration
        while len(self._items) > 0 and self._items[0] < expire_time:
            self._items.pop(0)

    def check(self):
        now = datetime.datetime.now()
        self._items.append(now)
        self._cleanup_expired(now)
        return len(self._items) <= self.items_limit
