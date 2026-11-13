import json
import os
import time
import urllib.request

from cloud.storage.core.tools.common.python.daemon import Daemon
from cloud.tasks.test.common.processes import register_process, kill_processes
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common

SERVICE_NAME = "backup-fault-proxy"


class BackupFaultLauncher:
    def __init__(self, nbs_port, primary_port, backup_port, cert_file, key_file):
        self._ports = yatest_common.PortManager()
        self.nbs_port, self.primary_port, self.backup_port, self.control_port = [
            self._ports.get_port() for _ in range(4)
        ]
        work = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(), sub_folder=""
        )
        ensure_path_exists(work)
        self.events = os.path.join(work, "backup-fault-events.jsonl")
        command = [
            yatest_common.binary_path("cloud/disk_manager/test/mocks/backup-fault-proxy/backup-fault-proxy"),
            "--nbs-upstream", "localhost:{}".format(nbs_port),
            "--primary-upstream", "http://localhost:{}".format(primary_port),
            "--backup-upstream", "http://localhost:{}".format(backup_port),
            "--cert", cert_file, "--key", key_file,
            "--grpc-port", str(self.nbs_port),
            "--primary-port", str(self.primary_port),
            "--backup-port", str(self.backup_port),
            "--control-port", str(self.control_port),
            "--events", self.events,
        ]
        self.daemon = Daemon(commands=[command], cwd=work, service_name=SERVICE_NAME)

    def start(self):
        self.daemon.start()
        register_process(SERVICE_NAME, self.daemon.pid)
        deadline = time.monotonic() + 30
        while True:
            try:
                with urllib.request.urlopen(
                    "http://localhost:{}".format(self.control_port), timeout=1
                ) as response:
                    json.load(response)
                return
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)
