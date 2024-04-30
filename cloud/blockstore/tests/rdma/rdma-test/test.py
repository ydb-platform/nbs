import logging

from cloud.blockstore.tests.python.lib.rdma import setup_rdma

_logger = logging.getLogger(__name__)

EXECUTE_EXTRA_TIMEOUT_SECS = 10


def check_network_connectivity(h1, h2, timeout=5):
    _logger.debug(
        "checking network connectivity %s->%s", h1.get_local_ip(), h2.get_local_ip()
    )

    cmd = ["ping", "-c", str(timeout), "-W", "1", h2.get_local_ip()]
    h1.execute(cmd, timeout=timeout + EXECUTE_EXTRA_TIMEOUT_SECS)


def check_rdma_connectivity(h1, h2, rdma_port=9999, timeout=10):
    _logger.debug(
        "checking rdma connectivity %s->%s", h1.get_local_ip(), h2.get_local_ip()
    )

    target_cmd = [
        h2.get_rdma_test_util_path(),
        "--test", "Target",
        "--host", h2.get_local_ip(),
        "--port", str(rdma_port),
        "--wait", "Poll",
        "--storage", "Null",
        "--verbose", "debug",
    ]

    initiator_cmd = [
        h1.get_rdma_test_util_path(),
        "--test", "Initiator",
        "--host", h2.get_local_ip(),
        "--port", str(rdma_port),
        "--wait", "Poll",
        "--storage", "Rdma",
        "--verbose", "debug",
        "--test-duration", str(timeout),
        "--connect-timeout", "120",
    ]

    p2 = h2.execute(target_cmd, wait=False)
    h1.execute(initiator_cmd)

    p2.terminate()

    # TODO: check why on left ci setup termination of rdma-test Target exits with 255
    p2.wait(timeout=5, check_exit_code=False)


def test_rdma():
    test_duration_sec = 10

    _logger.debug("starting rdma-test")

    h1, h2 = setup_rdma()
    check_network_connectivity(h1, h2)
    try:
        check_rdma_connectivity(h1, h2, timeout=test_duration_sec)
    except Exception as e:
        _logger.error("check_rdma_connectivity failed %s", e)
        check_network_connectivity(h1, h2)
        raise
