import logging
import os
import time
import requests
import urllib3

from cloud.blockstore.pylibs.ydb.tests.library.harness.daemon import Daemon
from cloud.blockstore.pylibs.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common


CONFIG_TEMPLATE = """
Port: {insecure_port}
NfsVhostHost: "localhost"
NfsVhostPort: {nfs_port}
"""


class _HttpProxyDaemon(Daemon):

    def __init__(self, config_file, working_dir, insecure_port=None):
        command = [yatest_common.binary_path(
            "cloud/filestore/tools/http_proxy/filestore-http-proxy")]
        command += [
            "--config", config_file
        ]

        super(_HttpProxyDaemon, self).__init__(
            command=command,
            cwd=working_dir,
            timeout=180)

        self.__insecure_port = insecure_port

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, tb):
        self.stop()

    @property
    def pid(self):
        return super(_HttpProxyDaemon, self).daemon.process.pid

    @property
    def insecure_port(self):
        return self.__insecure_port


def create_nfs_http_proxy(insecure_port, nfs_port):
    working_dir = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="")

    ensure_path_exists(working_dir)
    config_file = os.path.join(working_dir, 'http_proxy.txt')

    with open(config_file, "w") as f:
        f.write(CONFIG_TEMPLATE.format(
            insecure_port=insecure_port,
            nfs_port=nfs_port))

    return _HttpProxyDaemon(
        config_file,
        working_dir,
        insecure_port=insecure_port)


def wait_for_nfs_server_proxy(port):
    '''
    Ping NFS server proxy with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    url = str("http://localhost:%d/ping" % port)
    while True:
        try:
            r = requests.post(url, verify=False)
            r.raise_for_status()
            break
        except Exception:
            logging.warning("Failed to connect to NBS server proxy. Retry")
            time.sleep(1)
            continue
