# -*- coding: UTF-8 -*-

import datetime
import logging
import os
import re
import time

logger = logging.getLogger(__name__)


class OOMChecker(object):
    KERN_LOG = "/var/log/kern.log"
    BEGIN_OOM_RE = re.compile(r'(\w+\s+\d+\s+\d{2}:\d{2}:\d{2}) .* Task in .+ killed as a result of limit of')
    KILL_OOM_RE = re.compile(r'Memory cgroup out of memory: Kill process (\d+)')
    KILL_PROCESS_RE = re.compile(r'Killed process \d+ \((.+)\) total-vm:\S+, anon-rss:\S+, file-rss:\S+, shmem-rss:\S+')
    OOM_MESSAGES = [
        re.compile(r'memory: usage .+, limit .+, failcnt'),
        re.compile(r'anon: usage .+, limit .+, failcnt'),
        re.compile(r'Memory cgroup stats for '),
        re.compile(r'\[\s+pid\s+\]\s+uid\s+tgid total_vm\s+rss nr_ptes nr_pmds swapents oom_score_adj name'),
        re.compile(r'\[\d+\]\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\S+'),
        re.compile(r'oom_reaper: reaped process \d+ \(.+\), now anon-rss:\S+, file-rss:\S+, shmem-rss:\S+'),
    ]

    def __init__(self):
        super().__init__()
        self._logger = logger.getChild(self.__class__.__name__)
        self._pid = None
        self._oom_list = list()
        self._current_pid = None
        self._current_oom = list()
        self._current_process = None
        self._current_timestamp = None

    def check(self, pid):
        self._pid = pid
        try:
            file_size = os.path.getsize(self.KERN_LOG)
            read_bytes = 4096 * 1024  # 4Mb
            with open(self.KERN_LOG, "r") as fd:
                if file_size > read_bytes:
                    fd.seek(-read_bytes, os.SEEK_END)
                for line in fd.readlines():
                    line = line.strip()
                    if line:
                        self._check_line(line)
        except IOError as e:
            self._logger.error("Error check %s %r", self.KERN_LOG, e)
            self._logger.debug("Exception", exc_info=True)

        if self._current_pid == self._pid:
            self._oom_list.append((self._current_timestamp, self._current_process, self._current_oom))
        self._current_pid = None
        self._current_timestamp = None
        self._current_process = None
        self._current_oom = list()
        if not self._oom_list:
            return False
        return True

    @property
    def time(self):
        return self._oom_list[-1][0] if self._oom_list else None

    @property
    def service(self):
        return self._oom_list[-1][1] if self._oom_list else None

    @property
    def text(self):
        return "\n".join(self._oom_list[-1][2]) if self._oom_list else None

    @staticmethod
    def _parse_date(date_text):
        now = datetime.datetime.now()
        try:
            timestamp = datetime.datetime.strptime(date_text, "%b %d %H:%M:%S").replace(year=now.year)
            if timestamp > now:
                timestamp = timestamp.replace(year=now.year - 1)
            return int(time.mktime(timestamp.timetuple()))
        except ValueError:
            return int(time.mktime(now.timetuple()))

    def _check_line(self, line):
        begin_oom_result = self.BEGIN_OOM_RE.search(line)
        if begin_oom_result:
            if self._current_pid == self._pid:
                self._oom_list.append((self._current_timestamp, self._current_process, self._current_oom))
            self._current_pid = None
            self._current_process = None
            self._current_timestamp = self._parse_date(begin_oom_result.group(1))
            self._current_oom = list()
            self._current_oom.append(line)
            return
        kill_result = self.KILL_OOM_RE.search(line)
        if kill_result:
            self._current_pid = int(kill_result.group(1))
            self._current_oom.append(line)
            return
        process_result = self.KILL_PROCESS_RE.search(line)
        if process_result:
            self._current_process = process_result.group(1)
            self._current_oom.append(line)
            return
        for check in self.OOM_MESSAGES:
            if check.search(line):
                self._current_oom.append(line)
                break
