# -*- coding: UTF-8 -*-

from io import StringIO
import os


class ErrorCollector:
    # https://github.com/ydb-platform/nbs/blob/main/util/system/yassert.cpp
    ERROR_LINES = 3  # Message consist of three lines
    ERROR_PREFIXES = ["VERIFY failed:", "FAIL:", "VERIFY failed (", "FAIL ("]

    def __init__(self):
        self.errors = list()
        self.current = list()

    def add_lines(self, lines):
        for line in lines:
            self.add_line(line.strip())

    def add_line(self, line):
        current_len = len(self.current)
        if 0 < current_len < self.ERROR_LINES:
            self.current.append(line)
            if len(self.current) >= self.ERROR_LINES:
                self.errors.append(self.current)
                self.current = list()
            return
        for prefix in self.ERROR_PREFIXES:
            if line.startswith(prefix):
                self.current.append(line)


class StreamErrorCollector(ErrorCollector):

    def __init__(self):
        super().__init__()
        self.buf = StringIO.StringIO()

    def write(self, data):
        self.buf.write(data)
        self.buf.seek(0, os.SEEK_SET)
        line = None
        for line in self.buf.readlines():
            # Complete line
            if line.endswith('\r') or line.endswith('\n'):
                self.add_line(line)
        self.buf.truncate(0)
        # Return an incomplete line to the buffer
        if line is not None and not (line.endswith('\r') or line.endswith('\n')):
            self.buf.write(line)


class FileErrorCollector(ErrorCollector):

    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.load()

    def load(self):
        file_size = os.path.getsize(self.filename)
        read_bytes = 4096 * 1024  # 4Mb
        with open(self.filename, "r") as fd:
            if file_size > read_bytes:
                fd.seek(-read_bytes, os.SEEK_END)
            self.add_lines(fd.readlines())
