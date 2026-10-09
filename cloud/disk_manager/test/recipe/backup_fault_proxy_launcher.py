import time
import urllib.error
import urllib.request

import contrib.ydb.tests.library.common.yatest_common as yatest_common
from contrib.ydb.tests.library.harness.kikimr_runner import (
    ensure_path_exists,
    get_unique_path_for_current_test,
)
from cloud.storage.core.tools.common.python.daemon import Daemon
from cloud.tasks.test.common.processes import kill_processes, register_process


SERVICE_NAME = "backup_s3_fault_proxy"


class BackupFaultProxyLauncher:
    """Only connects an isolated recipe's loopback S3 emulator."""

    def __init__(self, upstream_port):
        self._port_manager = yatest_common.PortManager()
        self.port = self._port_manager.get_port()
        self.control_port = self._port_manager.get_port()
        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(), sub_folder=""
        )
        ensure_path_exists(working_dir)
        command = [
            yatest_common.binary_path(
                "cloud/disk_manager/test/mocks/s3_fault_proxy/cmd/s3_fault_proxy/s3_fault_proxy"
            ),
            "--listen", f"127.0.0.1:{self.port}",
            "--control-listen", f"127.0.0.1:{self.control_port}",
            "--upstream", f"http://127.0.0.1:{upstream_port}",
        ]
        self._daemon = Daemon(
            commands=[command], cwd=working_dir, service_name=SERVICE_NAME
        )

    def start(self):
        self._daemon.start()
        register_process(SERVICE_NAME, self._daemon.pid)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{self.control_port}/status", timeout=1
                ) as response:
                    if response.status == 200:
                        return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.1)
        raise RuntimeError("Backup fault proxy failed its readiness check")

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)
