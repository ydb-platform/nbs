import json
import logging
import subprocess
import time

from pathlib import Path

import yatest.common as common


RDMA_DEVICE = "rxe_repro0"
TEST_BINARY = (
    "cloud/blockstore/tools/testing/rdma-client-cq-stall/bin/"
    "nbs-rdma-client-cq-stall-test"
)


def _run(command, *, check=True, timeout=30):
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    logging.info(
        "command: %s\nexit code: %d\nstdout:\n%s\nstderr:\n%s",
        command,
        result.returncode,
        result.stdout,
        result.stderr,
    )
    if check:
        result.check_returncode()
    return result


def _guest_network():
    routes = json.loads(
        _run(["ip", "-j", "-4", "route", "show", "default"]).stdout
    )
    route = next(route for route in routes if "dev" in route)
    netdev = route["dev"]

    addresses = json.loads(
        _run(["ip", "-j", "-4", "address", "show", "dev", netdev]).stdout
    )
    address = next(
        info["local"]
        for interface in addresses
        for info in interface["addr_info"]
        if info["family"] == "inet" and info["scope"] == "global"
    )
    return netdev, address


def _wait_for_rxe():
    state = Path(
        "/sys/class/infiniband"
    ) / RDMA_DEVICE / "ports" / "1" / "state"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if state.exists() and "ACTIVE" in state.read_text():
            return
        time.sleep(0.1)
    raise RuntimeError(f"{RDMA_DEVICE} did not become active")


def test_blocked_response_handler_does_not_stall_rdma_queues():
    _run(["modprobe", "rdma_rxe"])
    netdev, address = _guest_network()
    _run(
        [
            "rdma",
            "link",
            "add",
            RDMA_DEVICE,
            "type",
            "rxe",
            "netdev",
            netdev,
        ]
    )

    try:
        _wait_for_rxe()
        _run(
            [
                "prlimit",
                "--memlock=unlimited:unlimited",
                "--",
                common.binary_path(TEST_BINARY),
                address,
            ],
            timeout=120,
        )
    finally:
        cleanup = _run(
            ["rdma", "link", "delete", RDMA_DEVICE],
            check=False,
        )
        if cleanup.returncode:
            logging.error("failed to remove temporary RXE device %s", RDMA_DEVICE)
