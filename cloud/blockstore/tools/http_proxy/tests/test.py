import os
import pytest
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import ydb.tests.library.common.yatest_common as yatest_common

from cloud.blockstore.tests.python.lib.nbs_http_proxy import create_nbs_http_proxy


def post(s, scheme, port, path):
    return s.post('{}://localhost:{}/{}'.format(scheme, port, path), verify=False)


@pytest.mark.parametrize("nbs_server_insecure", [True, False])
def test_ping(nbs_server_insecure):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    port_manager = yatest_common.PortManager()
    secure_port = port_manager.get_port()
    insecure_port = port_manager.get_port()
    nbs_port = (os.getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")
                if nbs_server_insecure
                else os.getenv("LOCAL_NULL_SECURE_NBS_SERVER_PORT"))
    certs_dir = os.getenv("TEST_CERT_FILES_DIR")

    with create_nbs_http_proxy(secure_port, insecure_port, nbs_port, nbs_server_insecure, certs_dir), \
            requests.Session() as s:
        s.mount('https://', HTTPAdapter(max_retries=Retry(total=10, backoff_factor=0.5)))
        s.mount('http://', HTTPAdapter(max_retries=Retry(total=10, backoff_factor=0.5)))

        r = s.post('https://localhost:{}/ping'.format(secure_port), verify=False)
        assert r.status_code == 200
        assert r.text == "{}"

        r = s.post('http://localhost:{}/ping'.format(insecure_port))
        assert r.status_code == 200
        assert r.text == "{}"
