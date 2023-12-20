import logging

from cloud.blockstore.tests.python.lib.rdma import setup_rdma

_logger = logging.getLogger(__name__)


def test_rdma():
    rdma_port = 9999
    test_duration_sec = 10

    _logger.debug("starting rdma-test")

    h1, h2 = setup_rdma()
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
        "--test-duration", str(test_duration_sec),
    ]

    p2 = h2.execute(target_cmd, wait=False)

    p1 = h1.execute(initiator_cmd, timeout=test_duration_sec + 5)
    _logger.debug("%s", p1.stdout.decode())

    p2.terminate()
    # TODO: check why on left ci setup termination of rdma-test Target exits with 255
    p2.wait(timeout=5, check_exit_code=False)
