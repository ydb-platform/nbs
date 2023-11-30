import StringIO
import errno
import fcntl
import logging
import os
import select
import subprocess
import time

logger = logging.getLogger(__name__)


class Command(object):

    def __init__(self, command, timeout):
        """
        :param command: Command to execute
        :type command: unicode
        :param timeout: Timeout of execution in seconds
        :type timeout: float
        """
        super(Command, self).__init__()
        self._logger = logger.getChild(self.__class__.__name__)
        self.command = command
        self.timeout = timeout
        self.execution_time = None  # type: float or None
        self.exit_code = None  # type: int or None
        self._process = None  # type: subprocess.Popen or None
        self._buffer_out = StringIO.StringIO()
        self._buffer_err = StringIO.StringIO()
        self._poller = select.poll()
        self._streams = list()
        self._fd2stream = dict()

    @staticmethod
    def _set_nonblock(stream):
        flags = fcntl.fcntl(stream.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(stream.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)

    def _read_nonblock(self, stream):
        try:
            return stream.read()
        except IOError as e:
            # Read can return: "IOError: [Errno 11] Resource temporarily unavailable"
            if e.errno != 11:
                self._logger.debug("Error read stream", exc_info=True)
        except OSError:
            self._logger.debug("Unknown error occurred during not block read %r", exc_info=True)
        return ""

    def _read_stream(self, stream):
        data = self._read_nonblock(stream)
        if len(data) == 0:
            return
        if stream == self._process.stdout:
            self._buffer_out.write(data)
        elif stream == self._process.stderr:
            self._buffer_err.write(data)

    def _init_streams(self):
        self._streams = [self._process.stdout, self._process.stderr]
        self._fd2stream = {fd.fileno(): fd for fd in self._streams}
        for fd in self._streams:
            self._poller.register(fd, select.POLLERR | select.POLLIN | select.POLLNVAL)
            self._set_nonblock(fd)

    def _check_streams(self):
        try:
            fds = self._poller.poll(100)
        except select.error, e:
            if e[0] == errno.EINTR:
                return
            raise
        if len(fds) > 0:
            for fd, ev in fds:
                if (ev & select.POLLIN) != 0:
                    self._read_stream(self._fd2stream[fd])
                else:
                    self._poller.unregister(fd)
                    stream = self._fd2stream[fd]
                    if stream in self._streams:
                        self._streams.remove(stream)

    def run(self):
        """Exec remote command
        :rtype: int or None
        :return: Exit code or None if timeout
        """
        self._logger.debug("Execute %r", self.command)
        try:
            self._process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
                shell=False
            )
        except Exception as e:
            self._logger.warning("Execution of %r failed with: %r", self.command, e)
            raise

        start_time = time.time()
        self._init_streams()
        while self._process.poll() is None:
            self._check_streams()
            self.execution_time = time.time() - start_time
            if self.execution_time >= self.timeout:
                self._logger.debug("Execution of command %r timeout", self.command)
                self._process.kill()
                self._buffer_out.seek(0, os.SEEK_SET)
                self._buffer_err.seek(0, os.SEEK_SET)
                return None, self._buffer_out.read(), self._buffer_err.read()
            time.sleep(0.1)
        self._check_streams()
        self.exit_code = self._process.returncode
        self._buffer_out.seek(0, os.SEEK_SET)
        self._buffer_err.seek(0, os.SEEK_SET)
        return self.exit_code, self._buffer_out.read(), self._buffer_err.read()
