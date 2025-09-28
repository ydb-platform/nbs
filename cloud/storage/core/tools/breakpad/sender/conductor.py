# -*- coding: UTF-8 -*-

import logging
import socket
import requests
from library.python.retry import retry


logger = logging.getLogger(__name__)


class ConductorError(Exception):
    pass


class Conductor:
    HTTP_TIMEOUT = 5
    API_URL = ""
    FILTERED_GROUPS = list()

    def __init__(self, hostname=None):
        self._logger = logger.getChild(self.__class__.__name__)
        self.hostname = hostname
        if self.hostname is None:
            self.hostname = socket.getfqdn()
        self.groups = list()
        self._update_groups()

    @retry(max_times=10)
    def _http_get(self, url):
        response = requests.get(url, timeout=self.HTTP_TIMEOUT)
        if response.status_code != 200:
            raise ConductorError("HTTP GET Error")
        return response.text

    def _update_groups(self):
        url = self.API_URL + self.hostname
        try:
            response = self._http_get(url)
        except ConductorError as e:
            logging.error("can't get conductor groups %s %r", url, e)
            return
        if response.startswith("Found "):
            logging.warning("conductor groups for host %s not found", self.hostname)
            return
        self.groups = filter(lambda x: len(x) > 0, response.splitlines())

    @property
    def primary_group(self):
        for group in self.groups:
            for name in self.FILTERED_GROUPS:
                if name in group:
                    break
            else:
                return group
        return "unknown"
