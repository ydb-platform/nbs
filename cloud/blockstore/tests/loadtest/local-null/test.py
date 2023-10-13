import os

import yatest.common as common

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.tests.python.lib.test_base import run_test


def test_tcp_load():
    nbs_port = int(os.getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT"))
    mon_port = int(os.getenv("LOCAL_NULL_MONITORING_PORT"))

    ret = run_test(
        "load",
        common.source_path(
            "cloud/blockstore/tests/loadtest/local-null/local.txt"),
        nbs_port,
        mon_port
    )

    return ret


def test_endpoint_load():
    nbs_port = int(os.getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT"))
    mon_port = int(os.getenv("LOCAL_NULL_MONITORING_PORT"))

    client = TClientConfig()
    client.NbdSocketSuffix = "_nbd"

    ret = run_test(
        "load",
        common.source_path(
            "cloud/blockstore/tests/loadtest/local-null/local-endpoint.txt"),
        nbs_port,
        mon_port,
        client_config=client,
    )

    return ret
