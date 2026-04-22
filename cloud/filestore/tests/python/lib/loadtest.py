import json
import logging
import requests
import tempfile

import yatest.common as common

from cloud.filestore.config.client_pb2 import TClientConfig
from cloud.storage.core.tests.common import expand_placeholders

logger = logging.getLogger(__name__)


def _expand_placeholders(config_path):
    with open(config_path, "r") as f:
        content = f.read()

    expanded = expand_placeholders(content)
    if expanded == content:
        return config_path

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False, dir=common.output_path()
    )
    tmp.write(expanded)
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
    config = _expand_placeholders(config)

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
