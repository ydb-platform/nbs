import os
import requests
import signal
import time
import logging

import yatest.common as yatest_common
from yatest.common.network import PortManager

from library.python.testing.recipe import declare_recipe, set_env


PID_FILE_NAME = "notify_service_mock_recipe.pid"


def start(argv):
    port_manager = PortManager()
    port = port_manager.get_port()
    set_env("NOTIFY_SERVICE_MOCK_PORT", str(port))

    args = [
        "python3",
        yatest_common.source_path("cloud/blockstore/tools/testing/notify-mock/__main__.py"),
        "--port", str(port),
        '--output-path', yatest_common.output_path() + '/notify-service.out',
    ]

    secure = '--secure' in argv

    if secure:
        certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')
        set_env("TEST_CERT_FILES_DIR", certs_dir)

        args += [
            '--ssl-cert-file', os.path.join(certs_dir, 'server.crt'),
            '--ssl-key-file', os.path.join(certs_dir, 'server.key'),
        ]

    mock = yatest_common.execute(args, wait=False)

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(mock.process.pid))

    url = None
    if secure:
        url = f'https://localhost:{port}/ping'
    else:
        url = f'http://localhost:{port}/ping'

    while True:
        try:
            r = requests.get(url, verify=False)
            r.raise_for_status()
            logging.info(f"Notify service: {r.text}")
            break
        except Exception:
            logging.warning("Failed to connect to Notify service. Retry")
            time.sleep(1)
            continue


def stop(argv):
    with open(PID_FILE_NAME) as f:
        pid = int(f.read())
        os.kill(pid, signal.SIGTERM)


if __name__ == "__main__":
    declare_recipe(start, stop)
