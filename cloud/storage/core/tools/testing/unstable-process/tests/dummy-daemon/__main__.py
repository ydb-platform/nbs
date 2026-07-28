import argparse
import socket
import time


def main():
    # A minimal stand-in for a real daemon managed by unstable-process
    # (launcher): binds the port the same way PortManager's is_port_free()
    # probes it (AF_INET6, '::') and idles until the launcher restarts it.
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.bind(('::', args.port))
    sock.listen(1)

    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
