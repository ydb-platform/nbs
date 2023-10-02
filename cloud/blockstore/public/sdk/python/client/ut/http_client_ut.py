import logging
import os
import pytest
import urllib3

try:
    from http.server import HTTPServer, BaseHTTPRequestHandler
except ImportError:
    # python2
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

from yatest.common import network

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client.http_client import HttpClient
from cloud.blockstore.public.sdk.python.client.credentials import ClientCredentials
from cloud.blockstore.public.sdk.python.client.error import ClientError
from cloud.blockstore.public.sdk.python.client.error_codes import EResult

from cloud.blockstore.public.sdk.python.client.ut.client_methods \
    import client_methods


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")


def _test_every_method(sync, insecure, timeout):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    cert_files_dir = os.getenv("TEST_CERT_FILES_DIR")
    root_certs_file = os.path.join(cert_files_dir, "server.crt")
    cert_file = os.path.join(cert_files_dir, "server.crt")
    cert_private_key_file = os.path.join(cert_files_dir, "server.key")

    credentials = None
    if not insecure:
        credentials = ClientCredentials(
            root_certs_file,
            cert_file,
            cert_private_key_file)

    port = int(os.getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PROXY_PORT"
                         if insecure
                         else "LOCAL_NULL_SECURE_NBS_SERVER_PROXY_PORT"))

    host = "localhost"
    server_mock = None
    if timeout is not None:
        with network.PortManager() as pm:
            port = pm.get_port()

        class NoOp(BaseHTTPRequestHandler):

            def do_GET(self):
                pass

            def do_POST(self):
                pass

        server_mock = HTTPServer((host, port), NoOp)  # noqa

    addr = "{}://{}:{}".format("http" if insecure else "https", host, port)

    http_client = HttpClient(
        addr,
        credentials=credentials,
        log=logger,
        timeout=timeout,
        connect_timeout=timeout)

    args_map = {
        "start_endpoint": {
            "UnixSocketPath": "./test.socket",
            "ClientId": "TestClientId"
        },
    }

    for client_method in client_methods:
        method_name = client_method[0]
        if not sync:
            method_name += "_async"
        request_name = "T%sRequest" % client_method[1]
        method = getattr(http_client, method_name)
        request_class = getattr(protos, request_name)
        request = request_class()

        request_dict = args_map.get(client_method[0])
        if request_dict:
            for key in request_dict:
                setattr(request, key, request_dict[key])

        te = None

        try:
            if sync:
                method(request)
            else:
                method(request).result()
        except ClientError as e:
            if e.code == EResult.E_TIMEOUT.value:
                te = e
            else:
                logger.error(e)
                pytest.fail(str(e))
        except Exception as e:
            logger.error(e)
            pytest.fail(str(e))

        if te is not None and timeout is None:
            pytest.fail(str(te))

        if te is None and timeout is not None:
            pytest.fail("expected timeout, got success")


@pytest.mark.parametrize("sync", ['sync', 'async'])
@pytest.mark.parametrize("insecure", ['insecure', 'secure'])
@pytest.mark.parametrize("timeout", [1, None])
def test_every_method(sync, insecure, timeout):
    _test_every_method(sync == 'sync', insecure == 'insecure', timeout=timeout)
