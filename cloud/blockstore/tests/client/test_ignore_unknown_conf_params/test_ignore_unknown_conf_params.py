import logging
import os
import subprocess
import yatest.common


def test_ignore_unknown_conf_params():

    BINARY_PATH = yatest.common.binary_path(
        "cloud/blockstore/apps/client/blockstore-client")

    CONF = yatest.common.test_source_path(
        "data/nbs-client-unknown-params.txt")

    # blockstore-client silently ignores unexisting --config file,
    # so make sure file exists at test path
    assert os.path.exists(CONF)

    WAIT_TIMEOUT = 100

    # use most simple 'ping' commant, wich parses --config file and can be run
    # without additional environment preparations
    args = [BINARY_PATH] + [
        "ping",
        "--config", CONF,
        "--timeout", "1",
        "--skip-cert-verification"]

    logging.info(f"""run: {args}""")

    process = subprocess.Popen(
        args,
        stdin=None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    try:
        outs, errs = process.communicate(timeout=WAIT_TIMEOUT)
    except subprocess.TimeoutExpired:
        process.kill()
        assert False, "blockstore-client reasonable timeout expired"

    # - yexception is thrown by protobuf parser ang logged in case of unknown
    #   params found
    # - E_CANCELLED logged expected in case of correct config and exit by
    #   --timeout option

    for stream in [outs, errs]:
        assert not ("yexception" in stream.decode("utf-8"))

    assert "Some unknown parameters ignored in config file" in errs.decode("utf-8")
    assert "E_CANCELLED" in outs.decode("utf-8")
