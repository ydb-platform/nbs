import argparse
import json
import logging
import signal
import ssl
import sys

from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread


def create_api(file):
    class NotifyAPI(BaseHTTPRequestHandler):

        def do_GET(self):
            logging.info('[NotifyAPI] Handle GET request')

            if self.path == '/ping':
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write('pong'.encode('utf-8'))
                return

            self.send_error(400)

        def do_POST(self):
            length = int(self.headers['content-length'])
            req = self.rfile.read(length).decode('utf-8')

            logging.info(f'[NotifyAPI] handle POST request: {self.path} {req}')

            if self.path == '/notify/v2/send':
                if self.headers.get('Authorization') is None:
                    self.send_error(403)
                    return
            elif self.path != '/notify/v1/send':
                self.send_error(400)
                return

            data = json.loads(req)

            if data.get('type') not in ['nbs.nonrepl.error', 'nbs.nonrepl.back-online']:
                self.send_error(400)
                return

            if data.get('cloudId') is None and data.get('userId') is None:
                self.send_error(400)
                return

            if data.get('data') is None:
                self.send_error(400)
                return

            json.dump(data, file)
            file.write('\n')
            file.flush()

            self.send_response(202)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "0")
            self.end_headers()
            self.wfile.write('{}'.encode('utf-8'))

    return NotifyAPI


def main(args, file):
    server = HTTPServer(('localhost', args.port), create_api(file))

    if args.ssl_cert_file is not None or args.ssl_key_file is not None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(args.ssl_cert_file, args.ssl_key_file)
        server.socket = context.wrap_socket(
            server.socket,
            server_side=True)

    def signal_handler(signum, frame):
        logging.info('shutdown...')
        server.shutdown()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    thread = Thread(target=server.serve_forever, name='notify-service')

    logging.info('start...')

    thread.start()
    thread.join()

    logging.info('done')

    return 0


def prepare_logging(args):
    log_level = logging.ERROR

    if args.silent:
        log_level = logging.INFO
    elif args.verbose:
        log_level = max(0, logging.ERROR - 10 * int(args.verbose))

    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="[%(levelname)s] [%(asctime)s] %(message)s")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--port', type=int, required=True)
    parser.add_argument('-O', '--output-path', type=str, default=None)
    parser.add_argument('--ssl-cert-file', type=str, default=None)
    parser.add_argument('--ssl-key-file', type=str, default=None)

    parser.add_argument("-s", '--silent', help="silent mode", default=0, action='count')
    parser.add_argument("-v", '--verbose', help="verbose mode", default=0, action='count')

    args = parser.parse_args()

    prepare_logging(args)

    if args.output_path is not None:
        with open(args.output_path, 'w') as file:
            exit(main(args, file))
    else:
        exit(main(args, sys.stdout))
