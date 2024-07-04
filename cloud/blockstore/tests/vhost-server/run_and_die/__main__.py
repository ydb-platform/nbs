import argparse
import subprocess
import sys
import json
from time import sleep


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cmd', required=True)
    args = parser.parse_args()

    server = subprocess.Popen(
        json.loads(args.cmd),
        stdin=None,
        stdout=sys.stdout,
        stderr=sys.stderr,
        bufsize=0,
        universal_newlines=True)

    sleep(1)
    print(server.pid)
