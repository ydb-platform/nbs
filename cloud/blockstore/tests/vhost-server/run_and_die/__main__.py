import argparse
import os
import subprocess
import sys
import json
from time import sleep


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cmd', required=True)
    parser.add_argument('--immediately', action='store_true', required=False)
    args = parser.parse_args()
    cmd = json.loads(args.cmd)
    cmd.append('--blockstore-service-pid=' + str(os.getpid()))

    server = subprocess.Popen(
        cmd,
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr,
        bufsize=0,
        universal_newlines=True)

    print(server.pid)
    if not args.immediately:
        sleep(2)
