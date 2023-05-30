from collections import deque
import json
import logging
import socket
import time

logger = logging.getLogger(__name__)


class QmpException(Exception):
    def __init__(self, qmp_result):
        message = "{}: {}".format(qmp_result["class"], qmp_result["desc"])
        super().__init__(message)


class QmpClient:
    timeout = 120

    def __init__(self, sock_path, vm_proc=None):
        self._events = deque()
        self._qmp_message = None
        self.sock_path = sock_path
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.settimeout(self.timeout)

        time_end = time.time() + self.timeout
        while True:
            try:
                self._sock.connect(sock_path)
                break
            except (FileNotFoundError, ConnectionRefusedError) as e:
                if time.time() > time_end:
                    logger.debug("%s: timed out", sock_path)
                    raise

                if vm_proc is not None:
                    res = vm_proc.poll()
                    if res is not None:
                        raise Exception('VM process exited with result: {}'.format(res))

                logger.debug("%s: %s: retrying", sock_path, e)
                time.sleep(1)

        logger.info("init for socket {}".format(sock_path))

        self.command("qmp_capabilities")

    def get_socket_path(self):
        return self.sock_path

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def command(self, command, **arguments):
        cmd = {"execute": command}
        if arguments:
            cmd["arguments"] = arguments

        qmp_cmd = json.dumps(cmd).encode("utf-8")
        logger.info("QMP> %s", qmp_cmd)
        self._sock.sendall(qmp_cmd)
        result = None
        while result is None:
            result = self._recv_data()

        logger.info("QMP< %s", result)
        return result

    def wait_event(self):
        while not self._events:
            res = self._recv_data()
            if res:
                raise Exception("Unexpected result: {}".format(res))
        return self._events.popleft()

    def _recv_data(self, events_only=False):
        result = None
        for message in self._recv_messages():
            logger.info("get message '{}'".format(message))
            if "event" in message.keys():
                self._events.append(message["event"])
            elif "QMP" in message.keys():
                self._qmp_message = message["QMP"]
            elif "return" in message.keys():
                if result or events_only:
                    raise Exception("Unexpected result: {}".format(message))
                result = message["return"]
            elif "error" in message.keys():
                raise QmpException(message["error"])
            elif "timestamp" in message.keys():
                pass
            elif "data" in message.keys():
                pass
            else:
                raise Exception("Unknown message type: {}".format(message))
        return result

    def _recv_messages(self):
        data = []
        while not (data and data[-1].endswith(b"\n")):
            data.append(self._sock.recv(4096))
        messages = []
        for line in b"".join(data).decode("utf-8").splitlines():
            try:
                messages.append(json.loads(line))
            except Exception as e:
                logger.error("Failed to parse message %s: %s\n", line, e)
                raise
        return messages
