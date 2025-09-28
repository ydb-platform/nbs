# -*- coding: UTF-8 -*-

import re


class CoredumpFormatter:
    CURRENT_THREAD_RE = re.compile(
        r'^\[Current thread is (\d+) \(LWP \d+\)\]$')
    NEW_STACK_RE = re.compile(r'^Thread (\d+) \(LWP \d+\):$')
    ADDR_RE = re.compile(r' (0x[0-9a-fA-F]+ in )')
    PATH_RE = re.compile(r' at ((.+?):(\d+)(?::(\d+))?)')
    ARCADIA_PATH = "/arcadia/"

    def __init__(self, coredump: str):
        self.info = list()
        self.stacks = list()
        self.last_thread = None
        self.last_stack = None
        self.current_thread = None
        self._process_coredump(coredump)

    def _check_current_thread(self, line: str):
        current_thread = self.CURRENT_THREAD_RE.match(line)
        if not current_thread:
            return
        self.current_thread = int(current_thread.group(1))

    def _check_new_stack(self, line):
        new_stack = self.NEW_STACK_RE.match(line)
        if not new_stack:
            return False
        thread_id = int(new_stack.group(1))
        if self.last_stack is not None:
            self.stacks.append((self.last_thread, self.last_stack))
        self.last_thread = thread_id
        self.last_stack = list()
        self.last_stack.append(line)
        return True

    def _process_info_line(self, line):
        self._check_current_thread(line)
        if line.startswith("[New LWP ") or line.startswith("#0 0x"):
            return
        if self._check_new_stack(line):
            return
        self.info.append(line)

    def _clean_addrs(self, line):
        for addr_match in self.ADDR_RE.findall(line):
            line = line.replace(addr_match, "")
        return line

    def _clean_path(self, path):
        idx = path.find(self.ARCADIA_PATH)
        if idx < 0:
            return path
        return path[idx + len(self.ARCADIA_PATH):]

    def _clean_paths(self, line):
        for path_match in self.PATH_RE.findall(line):
            original_path, path, lineno, pos = path_match
            new_path = self._clean_path(path) + " +" + lineno
            if pos:
                new_path += ":" + pos
            line = line.replace(original_path, new_path)
        return line

    def _process_stack_line(self, line):
        if self._check_new_stack(line):
            return
        line = self._clean_addrs(line)
        line = self._clean_paths(line)
        self.last_stack.append(line)

    def _process_line(self, line):
        if len(line) == 0:
            return
        if self.last_stack is None:
            self._process_info_line(line)
        else:
            self._process_stack_line(line)

    def _process_coredump(self, coredump):
        for line in coredump.split('\n'):
            self._process_line(line.strip())
        if self.last_stack is not None:
            self.stacks.append((self.last_thread, self.last_stack))
        self.stacks.sort()

    def format(self):
        backtraces = ["\n".join(line) for _, line in self.stacks]
        return "\n".join(self.info) + "\n\n" + ("\n\n".join(backtraces))
