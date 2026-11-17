import json
import logging
import requests
import tempfile

import yatest.common as common

from cloud.filestore.config.client_pb2 import TClientConfig
from cloud.filestore.tools.testing.loadtest.protos.loadtest_pb2 import TTestGraph
from cloud.storage.core.tests.common import expand_placeholders

from google.protobuf import text_format

logger = logging.getLogger(__name__)


def _expand_placeholders(config_path):
    with open(config_path, "r") as f:
        content = f.read()

    expanded = expand_placeholders(content)
    if expanded == content:
        return config_path

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, dir=common.output_path())
    tmp.write(expanded)
    tmp.close()
    return tmp.name


def _disable_index_consistency_validation(config_path):
    with open(config_path) as f:
        config = text_format.Parse(f.read(), TTestGraph())

    for test in config.Tests:
        if test.HasField("LoadTest"):
            logger.info(
                f"Disabling IndexConsistencyValidation for test='{test.LoadTest.Name or '<empty>'}' "
                "because sanitizer is enabled"
            )
            test.LoadTest.IndexConsistencyValidation.Enabled = False

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", dir=common.output_path(), delete=False) as f:
        f.write(text_format.MessageToString(config))
        return f.name


def run_load_test(
    name,
    config,
    port,
    auth=False,
    auth_token=None,
    mon_port=None,
    use_fast_shard_runtime=False,
):
    config = _expand_placeholders(config)
    if common.context.sanitize:
        # We've observed recurring test failures with enabled sanitizers because
        # compaction could not finish within the deadline. Instead of  extenting
        # the deadline, we disable the validation.
        config = _disable_index_consistency_validation(config)

    cmd = [
        common.binary_path("cloud/filestore/tools/testing/loadtest/bin/filestore-loadtest"),
        "--secure-port" if auth else "--port",
        str(port),
        "--tests-config",
        config,
    ]

    if use_fast_shard_runtime:
        cmd.append("--use-fast-shard-runtime")

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
