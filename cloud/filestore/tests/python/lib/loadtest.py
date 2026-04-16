import json
import logging
import os
import pathlib
import tempfile

import google.protobuf.text_format as text_format
import requests

import yatest.common as common

from cloud.filestore.config.client_pb2 import TClientConfig
from cloud.filestore.tools.testing.loadtest.protos.loadtest_pb2 import TTestGraph

logger = logging.getLogger(__name__)


def _patch_shm_paths(config_path):
    """Redirect SharedMemoryFilePath fields to the test output directory.

    The server's SharedMemoryBasePath is set to output_path()/shm by the
    service-kikimr recipe; client paths must match. Returns the original path
    if no SharedMemoryFilePath fields are present.
    """
    with open(config_path, "r") as f:
        graph = text_format.Parse(f.read(), TTestGraph())

    shm_base = None
    changed = False
    for entry in graph.Tests:
        if entry.WhichOneof("Test") != "LoadTest":
            continue
        load_test = entry.LoadTest
        if load_test.WhichOneof("Specs") != "DatashardLikeLoadSpec":
            continue
        old_path = load_test.DatashardLikeLoadSpec.SharedMemoryFilePath
        if not old_path:
            continue
        if shm_base is None:
            shm_base = os.path.join(common.output_path(), "shm")
            os.makedirs(shm_base, exist_ok=True)
        new_path = str(pathlib.Path(shm_base) / pathlib.Path(old_path).name)
        load_test.DatashardLikeLoadSpec.SharedMemoryFilePath = new_path
        changed = True

    if not changed:
        return config_path

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False, dir=common.output_path()
    )
    tmp.write(text_format.MessageToString(graph))
    tmp.close()
    return tmp.name


def run_load_test(
    name,
    config,
    port,
    auth=False,
    auth_token=None,
    mon_port=None,
):
    config = _patch_shm_paths(config)

    cmd = [
        common.binary_path("cloud/filestore/tools/testing/loadtest/bin/filestore-loadtest"),
        "--secure-port" if auth else "--port",
        str(port),
        "--tests-config",
        config,
    ]

    if auth and auth_token is not None:
        client_config_path = common.output_path() + "/client.txt"
        with open(client_config_path, "w") as f:
            client_config = TClientConfig()
            client_config.RootCertsFile = common.source_path("cloud/filestore/tests/certs/server.crt")
            client_config.AuthToken = auth_token
            f.write(str(client_config))
        cmd.extend(["--config", client_config_path])

    logger.info("launching load test: " + " ".join(cmd))
    res = common.execute(cmd)

    if mon_port is not None:
        url = f"http://localhost:{mon_port}/counters/counters=filestore/json"
        r = requests.get(url, timeout=10)
        r.raise_for_status()

        logger.info(json.dumps(r.json(), indent=4))

    return res
