import os
import pytest

from cloud.blockstore.public.sdk.python.client import CreateClient
from cloud.blockstore.public.sdk.python.client.discovery import CreateDiscoveryClient


@pytest.mark.parametrize("client_type", ['regular', 'discovery'])
def test_insecure(client_type):
    port = os.getenv("LOCAL_KIKIMR_INSECURE_NBS_SERVER_PORT")

    if client_type == 'regular':
        client = CreateClient("localhost:{}".format(port))
    else:
        client = CreateDiscoveryClient("localhost:{}".format(port))

    client.create_volume("vol0", 4096, 1000)
    assert client.list_volumes() == ["vol0"]
