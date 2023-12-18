import logging

import yatest.common as common

from cloud.filestore.config.client_pb2 import TClientConfig

logger = logging.getLogger(__name__)


def run_load_test(name, config, port, auth=False, auth_token=None):
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
    return common.execute(cmd)
