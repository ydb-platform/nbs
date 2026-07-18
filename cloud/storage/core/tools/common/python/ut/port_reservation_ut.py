import os

import library.python.filelock
import yatest.common as yc

from cloud.storage.core.tools.common.python.port_reservation import (
    PortManager,
    PortReservation,
)


def setup_sync_dir():
    sync_dir = yc.output_path("port_sync_dir")
    os.makedirs(sync_dir, exist_ok=True)
    os.environ["PORT_SYNC_PATH"] = sync_dir
    return sync_dir


def get_lock_file_path(sync_dir, port):
    return os.path.join(sync_dir, str(port))


def assert_lock_failure(sync_dir, port):
    lock_file_path = get_lock_file_path(sync_dir, port)
    assert os.path.exists(lock_file_path)

    lock = library.python.filelock.FileLock(lock_file_path)
    assert not lock.acquire(blocking=False)


def assert_lock_success(sync_dir, port):
    lock_file_path = get_lock_file_path(sync_dir, port)

    lock = library.python.filelock.FileLock(lock_file_path)
    assert lock.acquire(blocking=False)
    lock.release()


def test_port_manager():
    sync_dir = setup_sync_dir()
    pm = PortManager()

    ports = [pm.reserve_port() for _ in range(3)]

    # Ports should be reserved by the underlying PortManager
    for port in ports:
        assert_lock_failure(sync_dir, port)

    assert pm.list_reserved_ports() == ports

    # a released port is no longer reserved and can be reserved again
    pm.release_port(ports[1])
    assert pm.list_reserved_ports() == [ports[0], ports[2]]
    assert_lock_success(sync_dir, ports[1])


def test_port_reservation_and_release():
    sync_dir = setup_sync_dir()
    pm = PortManager()
    port = pm.reserve_port()
    pm.release_port(port)

    reservation = PortReservation([port])
    reservation.reserve_ports()
    assert_lock_failure(sync_dir, port)

    reservation.release()
    assert_lock_success(sync_dir, port)
