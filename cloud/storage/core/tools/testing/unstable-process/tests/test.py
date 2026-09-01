import errno
import os
import socket
import threading
import time

import yatest.common as yc

import library.python.filelock

from cloud.storage.core.tools.common.python.daemon import Daemon, DaemonError
from cloud.storage.core.tools.common.python.port_reservation import PortManager


RESTART_INTERVAL = 1


def launcher_command():
    return [yc.binary_path(
        "cloud/storage/core/tools/testing/unstable-process/storage-unstable-process")]


def dummy_daemon_command():
    return [yc.binary_path(
        "cloud/storage/core/tools/testing/unstable-process/tests/dummy-daemon/dummy-daemon")]


class PortStealer(threading.Thread):
    """Mimic of a concurrent suite capturing the port.

    Non-blocking flock on <sync>/<port> (as PortManager does), then an attempt
    to bind the port - the same check PortManager's is_port_free() performs,
    except the socket is kept: a successful bind means the port was both
    unlocked and unbound, and the stealer holds the flock and the socket
    (as the concurrent suite's PortManager + daemon would) and sets `stolen`.
    If the port is in use, the flock is released and the attempt is retried in
    a tight loop to reproduce the race as aggressively as possible.

    """

    def __init__(self, port, sync_dir):
        super().__init__(daemon=True)
        self._port = port
        self._lock_path = os.path.join(sync_dir, str(port))
        self._should_stop = threading.Event()
        self.stolen = threading.Event()

    def stop(self):
        self._should_stop.set()
        self.join()

    def run(self):
        while not self._should_stop.is_set():
            lock = library.python.filelock.FileLock(self._lock_path)
            if not lock.acquire(blocking=False):
                # flock is held (steady state once the launcher reserved it):
                # back off slightly so spinning stealers don't starve slow
                # runners, while the free-port path below stays as tight as
                # possible
                time.sleep(0.001)
                continue

            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            try:
                sock.bind(('::', self._port))
            except OSError as e:
                sock.close()
                lock.release()
                if e.errno != errno.EADDRINUSE:
                    raise
                continue

            # port was both unlocked and unbound: captured exactly like a
            # foreign PortManager + daemon would
            sock.listen(1)
            self.stolen.set()

            self._should_stop.wait()

            sock.close()
            lock.release()
            return


class Env:
    def __init__(self):
        # everything lands under the test output dir, so the launcher's
        # captured std streams and the lock files survive as test artifacts
        self.dir = yc.output_path("unstable_process_test")
        self.sync_dir = os.path.join(self.dir, "port_sync_dir")
        os.makedirs(self.sync_dir, exist_ok=True)
        # inherited by the launcher process (and read by PortReservation and
        # any PortManager constructed without an explicit sync_dir)
        os.environ["PORT_SYNC_PATH"] = self.sync_dir
        self._launchers = []
        self._stealers = []

    def start_launcher(self, port, reserve):
        command = launcher_command() + [
            "--restart-interval", str(RESTART_INTERVAL),
            "--cmdline", " ".join(dummy_daemon_command() + ["--port", str(port)]),
        ]
        if reserve:
            command += ["--reserve-port", str(port)]

        launcher = Daemon(
            commands=[command], cwd=self.dir, service_name="launcher")
        launcher.start()
        self._launchers.append(launcher)

        return launcher

    def start_stealers(self, port, count=4):
        for _ in range(count):
            stealer = PortStealer(port, self.sync_dir)
            stealer.start()
            self._stealers.append(stealer)

    def stolen(self):
        return any(stealer.stolen.is_set() for stealer in self._stealers)

    def cleanup(self):
        for launcher in self._launchers:
            try:
                if launcher.is_alive():
                    launcher.stop()
            except DaemonError:
                pass

        for stealer in self._stealers:
            stealer.stop()


def port_listening(port):
    # A bind-based probe can briefly capture the port and make the daemon's
    # concurrent bind fail with EADDRINUSE. Connecting verifies readiness
    # without competing with the daemon for ownership of the port.
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.1)
        return sock.connect_ex(("::1", port)) == 0


def wait_for(predicate, timeout=60, step=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


# See issue #6518 for details
def test_cannot_steal_reserved_port():
    env = Env()
    try:
        pm = PortManager()
        port = pm.reserve_port()

        # maximum pressure: stealers hammer the flock before the launcher
        # even starts
        env.start_stealers(port)

        assert not env.stolen(), \
            "port stealer captured the port before launcer is started"

        launcher = env.start_launcher(port, reserve=True)
        assert wait_for(lambda: port_listening(port))
        assert launcher.is_alive(), \
            "launcher died shortly after start"

        # corresponds to recipe finished its start
        pm.release_port(port)

        # let several restart gaps happen under attack
        time.sleep(RESTART_INTERVAL * 5)

        assert not env.stolen(), \
            "port stealer captured the port after despite the reservation"
        assert launcher.is_alive(), \
            "launcher died; the daemon likely failed to rebind its port"
    finally:
        env.cleanup()


def test_steals_port_without_reservation():
    # Proves the stealer (and therefore the test above) can actually reproduce
    # the original race. Without --reserve-port the first restart gap is
    # unprotected: the stealer takes the flock, finds the port free, binds it.
    # The daemon then fails to rebind (EADDRINUSE) and exits, and the launcher
    # terminates with exit code 3 ('subprocess unexpectedly exited').
    #
    # See issue #6518 for details
    env = Env()
    try:
        pm = PortManager()
        port = pm.reserve_port()

        env.start_stealers(port)

        launcher = env.start_launcher(port, reserve=False)
        assert wait_for(lambda: port_listening(port))
        pm.release_port(port)

        assert wait_for(env.stolen), \
            "port stealer failed to reproduce the race with unreserved ports"
        assert wait_for(lambda: not launcher.is_alive())
        assert launcher.returncode == 3
    finally:
        env.cleanup()
