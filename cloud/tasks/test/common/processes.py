import os
import signal
import logging

from yatest.common.process import _Execution

logger = logging.getLogger()


# Creating an object similar to Process that is expected by
# _Execution.verify_no_coredumps()
class _BareProcess(object):
    def __init__(
        self,
        command,
        pid,
    ):
        self.command = command
        self.pid = pid
        self.returncode = -1


# We dance around pid files because start and stop in
# declare_recipe(start, stop) don't share memory.
def register_process(service_name: str, pid: int):
    pids_file_name = _get_pids_file_name(service_name)

    command_line = ''
    with open("/proc/{0}/cmdline".format(pid), "r") as cl:
        command_line = cl.read().replace('\0', ' ')
    with open(pids_file_name, "a") as f:
        f.write(str(pid) + " " + command_line + "\n")


def kill_processes(service_name: str):
    pids_file_name = _get_pids_file_name(service_name)
    if not os.path.exists(pids_file_name):
        return

    with open(pids_file_name) as f:
        exception = None
        pids = []
        for s in f.readlines():
            pid_and_command = s.split()
            pid = int(pid_and_command[0])
            command = pid_and_command[1:]
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError as e:
                logger.debug("Manual recovery of core dump for %d '%s'", pid,
                             command)
                process = _BareProcess(command, pid)
                execution = _Execution(command, process, None, None)
                execution.verify_no_coredumps()
                pids.append(pid)
                exception = e
        if exception is not None:
            setattr(exception, "pids", pids)
            raise exception


def _get_pids_file_name(service_name: str):
    return "cloud_tasks_test_common_%s.pids" % service_name
