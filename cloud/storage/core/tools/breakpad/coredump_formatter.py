# -*- coding: UTF-8 -*-

import re


class CoredumpFormatter(object):
    NEW_STACK_RE = re.compile(r'^Thread (\d+) \(LWP \d+\):$')
    ADDR_RE = re.compile(r' (0x[0-9a-fA-F]+ in )')
    PATH_RE = re.compile(r' at ((.+?):(\d+)(?::(\d+))?)')
    ARCADIA_PATH = "/arcadia/"

    def __init__(self):
        super(CoredumpFormatter, self).__init__()
        self.info = list()
        self.stacks = list()
        self.current_thread = None
        self.current_stack = None

    def _check_new_stack(self, line):
        new_stack = self.NEW_STACK_RE.match(line)
        if not new_stack:
            return False
        thread_id = int(new_stack.group(1))
        if self.current_stack is not None:
            self.stacks.append((self.current_thread, self.current_stack))
        self.current_thread = thread_id
        self.current_stack = list()
        self.current_stack.append(line)
        return True

    def _process_info_line(self, line):
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
        self.current_stack.append(line)

    def _process_line(self, line):
        if len(line) == 0:
            return
        if self.current_stack is None:
            self._process_info_line(line)
        else:
            self._process_stack_line(line)

    def format(self, coredump):
        for line in coredump.split('\n'):
            self._process_line(line.strip())
        if self.current_stack is not None:
            self.stacks.append((self.current_thread, self.current_stack))
        self.stacks.sort()
        return "\n".join(self.info) + "\n\n" + ("\n\n".join(["\n".join(stack_lines) for _, stack_lines in self.stacks]))
