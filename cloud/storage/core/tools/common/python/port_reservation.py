import logging
import os
import time

import library.python.filelock
import yatest.common.network as ya_network


class PortManager:
    """Wrapper over a yatest PortManager that memorizes allocated ports.

    yatest PortManager reserves a port by holding an flock on
    <PORT_SYNC_PATH>/<port>, but that flock only lives as long as the
    allocating process (where PortManager is locatedf). When the process
    that binds the port is wrapped in a restarting launcher (unstable-process),
    the ports must stay reserved for the launcher's whole lifetime instead:
    every restart briefly frees the sockets, and during that gap a concurrent
    suite's PortManager could otherwise grab the same ports.

    Code that allocates such ports should do it via reserve_port() and pass
    list_reserved_ports() to the unstable-process via --reserve-port option, which re-takes
    and holds the flocks via PortReservation below.

    Introduced to fix a race with unreserved ports (see issue #6518 for details)
    """

    def __init__(self):
        # the underlying PortManager picks up the sync dir from the
        # PORT_SYNC_PATH environment variable
        self._port_manager = ya_network.PortManager()
        self._reserved_ports = []

    def reserve_port(self):
        port = self._port_manager.get_port()
        self._reserved_ports.append(port)
        return port

    def list_reserved_ports(self):
        return list(self._reserved_ports)

    def release_port(self, port):
        # a released port is no longer reserved
        if port in self._reserved_ports:
            self._reserved_ports.remove(port)
        self._port_manager.release_port(port)

    def release(self):
        self._reserved_ports = []
        self._port_manager.release()


class PortReservation:
    """Re-holds port flocks before the launched process's sockets are freed.

    Re-takes the same flocks the allocating PortManager held - same path
    convention and same library.python.filelock.FileLock, so mutual exclusion
    with any PortManager sharing PORT_SYNC_PATH is preserved - and keeps them
    until the holding process exits.

    reserve_ports() is called synchronously right before every terminate/kill.
    A single non-blocking flock attempt is not enough there: a concurrent
    PortManager takes the flock for a moment while it bind-checks the port
    (the check fails while our child is alive, so the flock comes right
    back), and if our only attempt happens to land in that moment we would
    free the sockets with the port unreserved - exactly the race we are
    fixing. So each call retries in short steps for up to grace_seconds.

    A holder that does not go away within the grace period is not transient -
    it is the allocating process itself, still alive and still holding the
    flock. In that case the port is protected by that process for as long as
    it lives, so it is safe to proceed with the restart - the next
    reserve_ports() call retries and picks the flock up.

    Introduced to fix a race with unreserved ports (see issue #6518 for details)
    """

    def __init__(self, ports, grace_seconds=2, retry_step_seconds=0.001):
        self._sync_dir = os.environ.get('PORT_SYNC_PATH')
        assert self._sync_dir

        assert ports
        self._ports = sorted(set(ports))

        self._locks = {}
        self._grace_seconds = grace_seconds
        self._retry_step_seconds = retry_step_seconds
        self._reserve_done = False

    def reserve_ports(self):
        # All acquisition normally happens on the first call (right before
        # the first restart), ports whose flocks are still held by a
        # long-lived process (see the class docstring) are retried on every
        # subsequent call until acquired

        if self._reserve_done:
            return

        pending = set(self._ports) - set(self._locks)
        deadline = time.monotonic() + self._grace_seconds
        while True:
            for port in sorted(pending):
                lock = library.python.filelock.FileLock(
                    os.path.join(self._sync_dir, str(port)))
                if lock.acquire(blocking=False):
                    # Keep the fd open -> lock stays held until we exit
                    self._locks[port] = lock
                    pending.discard(port)
                    logging.info('reserved port %d (%s)', port, lock.path)

            if not pending:
                self._reserve_done = True
                break

            if time.monotonic() >= deadline:
                # Not transient: the flocks are held by a long-lived process
                # (normally the allocating one, which itself keeps the ports
                # protected).
                #
                # Proceed and retry on the next call
                logging.info(
                    'ports %s are still flocked by another process; '
                    'will retry before the next restart', sorted(pending))
                break

            time.sleep(self._retry_step_seconds)

    def release(self):
        while self._locks:
            _, lock = self._locks.popitem()
            lock.release()
