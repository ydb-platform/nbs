import os
import pytest
import socket

from cloud.blockstore.public.sdk.python.client import CreateClient, ClientCredentials
from cloud.blockstore.public.sdk.python.client.discovery import CreateDiscoveryClient

from yatest.common.network import PortManager


def create_discovery_client(endpoint, credentials):
    port_manager = PortManager()
    port = port_manager.get_port()

    endpoints = [
        "localhost:1",
        "localhost:%d" % port,
        endpoint]

    sock = socket.socket()
    sock.bind(("", port))
    sock.listen(1)

    return CreateDiscoveryClient(endpoints, credentials=credentials)


@pytest.mark.parametrize("client_type", ['regular', 'discovery'])
def test_secure(client_type):
    port = os.getenv("LOCAL_KIKIMR_SECURE_NBS_SERVER_PORT")

    endpoint = "localhost:{}".format(port)
    credentials = ClientCredentials(
        root_certs_file=os.path.join(os.getenv("TEST_CERT_FILES_DIR"), "server.crt"))

    if client_type == 'regular':
        client = CreateClient(endpoint, credentials=credentials)
    else:
        client = create_discovery_client(endpoint, credentials)

    client.create_volume("vol0", 4096, 1000)
    assert client.list_volumes() == ["vol0"]
