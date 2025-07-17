import argparse
import json
import logging
import signal
import sys

from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread


def _prepare_logging(verbose):
    log_level = {
        "error": logging.ERROR,
        "warn": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }[verbose]

    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="[%(levelname)s] [%(asctime)s] %(message)s")


def __set_comm(proc_name):
    with open('/proc/self/comm', 'w') as f:
        f.write(proc_name)


class _DeviceChunk:

    def __init__(self, s: str):
        i = s.rfind(':')
        if i == -1 or i == 0:
            raise argparse.ArgumentTypeError(f"invalid format: {s}")

        j = s[0:i-1].rfind(':')
        if j == -1:
            raise argparse.ArgumentTypeError(f"invalid format: {s}")

        self.offset = int(s[i+1:])
        self.size = int(s[j+1:i])
        self.path = s[:j]


class _App:

    def __init__(self):
        self.exit_code = 0

    def terminate(self, exit_code):
        self.exit_code = exit_code
        exit(exit_code)


def _create_handler(args, app: _App):

    class Handler(BaseHTTPRequestHandler):

        def do_GET(self):
            self.log_request()

            if self.path == '/ping':
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write('pong'.encode('utf-8'))
                return

            if self.path == '/describe':
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "devices": [[d.path, d.size, d.offset] for d in args.device]
                }).encode('utf-8'))
                return

            self.send_error(400)

        def do_POST(self):
            self.log_request()

            length = int(self.headers['content-length'])
            req = self.rfile.read(length).decode('utf-8')

            if self.path == '/terminate':
                data = json.loads(req)

                exit_code = data.get('exit_code', -1)
                logging.info(f'exit with {exit_code}...')
                app.terminate(exit_code)
                return

            self.send_error(400)

    return Handler


def _run_server(args):
    __set_comm("vhost-" + args.disk_id)

    app = _App()

    server = HTTPServer(('localhost', args.port), _create_handler(args, app))

    t0 = datetime.now()

    def signal_handler(signum, frame):
        if signum == signal.SIGUSR1:
            # TODO
            print(json.dumps({
                "elapsed_ms": int((datetime.now() - t0).total_seconds() * 1000)
            }))
            return

        logging.info('shutdown...')
        server.shutdown()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGUSR1, signal_handler)

    thread = Thread(target=server.serve_forever, name='server')

    logging.info('start...')

    thread.start()
    thread.join()

    logging.info('done')

    return app.exit_code


def main():
    parser = argparse.ArgumentParser()

    # custom options

    parser.add_argument('--port', type=int, required=True)

    # vhost-server options

    parser.add_argument(
        "-v",
        '--verbose',
        help="output level for diagnostics messages",
        default="debug",
        type=str)

    parser.add_argument("-s", '--socket-path', type=str, metavar="FILE", required=True)

    parser.add_argument(
        "-i",
        "--serial",
        type=str,
        metavar="STR",
        help="disk serial",
        required=True)

    parser.add_argument("--disk-id", type=str, metavar="STR", help="disk id")

    parser.add_argument(
        "--socket-access-mode",
        help="access mode for endpoint socket files",
        type=int,
        metavar="INT",
        default=432)

    parser.add_argument(
        "--client-id",
        type=str,
        metavar="STR",
        help="client id",
        default="vhost-server")

    parser.add_argument(
        "--device",
        required=True,
        type=_DeviceChunk,
        metavar="STR",
        action="append",
        help="specify device string path:size:offset "
        "(e.g. /dev/vda:1000000:0, rdma://host:10020/abcdef:1000000:0)")

    parser.add_argument(
        "--device-backend",
        type=str,
        metavar="STR",
        help="specify device backend",
        choices=["aio", "rdma", "null"],
        default="aio")

    parser.add_argument(
        "-r",
        "--read-only",
        action='store_true',
        help="read only mode")

    parser.add_argument(
        "--no-sync",
        action='store_true',
        help="do not use O_SYNC")

    parser.add_argument(
        "--no-chmod",
        action='store_true',
        help="do not chmod socket")

    parser.add_argument(
        "-B",
        "--batch-size",
        type=int,
        metavar="INT",
        default=1024)

    parser.add_argument(
        "--block-size",
        help="size of block device",
        type=int,
        metavar="INT",
        default=512)

    parser.add_argument(
        "-q",
        "--queue-count",
        type=int,
        metavar="INT",
        default=0)

    parser.add_argument(
        "--log-type",
        type=str,
        metavar="STR",
        choices=["json", "console"],
        default="json")

    parser.add_argument(
        "--rdma-queue-size",
        help="Rdma client queue size",
        type=int,
        metavar="INT",
        default=256)

    parser.add_argument(
        "--rdma-max-buffer-size",
        help="Rdma client queue size",
        type=int,
        metavar="INT",
        default=4*1024**2 + 4096)

    parser.add_argument(
        "--wait-after-parent-exit",
        help="How many seconds keep alive after the parent process is exited",
        type=int,
        metavar="INT")

    parser.add_argument(
        "--blockstore-service-pid",
        help="PID of blockstore service",
        type=int,
        metavar="INT")

    args = parser.parse_args()

    _prepare_logging(args.verbose)

    return _run_server(args)


if __name__ == '__main__':
    exit(main())
