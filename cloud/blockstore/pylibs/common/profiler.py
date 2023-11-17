import logging
import os
import paramiko
import subprocess
import sys
import uuid

from contextlib import contextmanager


class Profiler:
    def __init__(self, logger):
        paramiko.util.log_to_file(f'ssh-{uuid.uuid1()}.log', level=logging.DEBUG)

        self.logger = logger
        self.ips = []
        self.tcpdump_file = open(f'tcpdump-{uuid.uuid1()}.txt', 'w')
        self.tcpdump_process = subprocess.Popen(
            'sudo tcpdump -v',
            stdin=None,
            stdout=self.tcpdump_file,
            stderr=sys.stderr,
            shell=True)

    def add_ip(self, ip: str):
        self.ips.append(ip)

    def stop(self):
        for ip in self.ips:
            ping_cmd = ['ping6', '-c', '3', ip]
            self.logger.info(f'Ping {ip}')
            self.logger.info(f'Result: {subprocess.call(ping_cmd) == 0}')

        os.system(f'sudo kill {self.tcpdump_process.pid}')
        self.logger.info(f'See tcpdump in {self.tcpdump_file.name} file')
        self.tcpdump_file.close()


class ProfilerStub:
    def __init__(self):
        pass

    def add_ip(self, ip: str):
        pass

    def stop(self):
        pass


@contextmanager
def make_profiler(logger, stub: bool):

    profiler = ProfilerStub() if stub else Profiler(logger)
    try:
        yield profiler
    finally:
        profiler.stop()
