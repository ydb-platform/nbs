import argparse
import json
import logging
import os
import requests
import sys
import time

from datetime import datetime, timedelta

RESTART_DELAY = 5
RESTART_DELAY_CONTROL = 60
DATE_FORMAT = "%Y-%m-%d %H:%M:%S %Z"


def run_mock(args):
    port = os.getenv('PSSH_MOCK_PORT')
    if port is not None:
        r = requests.post(f'http://localhost:{port}/pssh', data=json.dumps({
            'cmd': args.command,
            'host': args.target,
            'format': args.format
        }))
        r.raise_for_status()

        logging.debug(f"response: {r.text}")

        j = r.json()

        if j.get('please_touch_yubikey', False):
            print("Issuing new session certificate", file=sys.stderr)
            print("Please touch yubikey", file=sys.stderr, end='')
            time.sleep(5)
            print("OK", file=sys.stderr)

        if args.format == 'text':
            print(f"{args.target}:")

            rc = j.get('return_code', 0)
            print(f'OUT[{rc}]:')
            for line in j.get('stdout', []):
                print(line)

            if rc != 0:
                print(f'ERR[{rc}]:', file=sys.stderr)

            stderr = j.get('stderr')
            if rc == 0 and stderr is None:
                stderr = ["Completed 1/1"]

            for line in stderr:
                print(line, file=sys.stderr)

            if rc != 0:
                print('command execution on one or more hosts failed',
                      file=sys.stderr)

            return 0

        if args.format == 'json':
            json.dump({
                "host": args.target,
                "stdout": '\n'.join(j.get('stdout')) + '\n',
                "stderr": '\n'.join(j.get('stderr')) + '\n',
                "error": "",
                "exit_status": j.get('return_code', 0)
            }, sys.stdout)

            return 0

        return 1

    if args.command == 'hostname':  # test
        json.dump({
            "host": args.target,
            "stdout": args.target,
            "stderr": "",
            "error": "",
            "exit_status": 0
        }, sys.stdout)
        return 0

    delay = RESTART_DELAY_CONTROL if args.target.find(
        "control") >= 0 else RESTART_DELAY
    if args.target.find("BROKEN") >= 0:
        uptime = delay / 2
    else:
        uptime = delay

    now_ts = datetime.strftime(datetime.utcnow(), DATE_FORMAT)
    start_ts = datetime.strftime(datetime.utcnow() - timedelta(seconds=uptime),
                                 DATE_FORMAT)

    json.dump({
        "host": args.target,
        "stdout": '\n'.join([
            f"Active: active (running) since SomeDay {start_ts}UTC; some days ago",
            f"{now_ts}UTC",
            "OK"
        ]),
        "stderr": "",
        "error": "",
        "exit_status": 0
    }, sys.stdout)

    return 0


def scp_mock(src, dst):
    pass


def list_mock(target):
    port = os.getenv('PSSH_MOCK_PORT')
    if port is not None:
        r = requests.post(f'http://localhost:{port}/pssh', data=json.dumps({
            'cmd': 'list',
            'host': target,
        }))
        r.raise_for_status()

        logging.debug(f"response: {r.text}")

        print(r.text)
        return 0

    return 1


def prepare_logging():
    log_level = int(os.getenv('PSSH_MOCK_LOG_LEVEL', str(logging.DEBUG)))

    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="[%(levelname)s] [%(asctime)s] %(message)s")


def main():
    prepare_logging()

    if sys.argv[1] == 'run':
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--format",
            choices=['json', 'text'],
            type=str,
            default='text')

        parser.add_argument(
            "command",
            type=str)

        parser.add_argument(
            "target",
            type=str)

        args = parser.parse_args(sys.argv[2:])

        return run_mock(args)

    if sys.argv[1] == 'scp':
        return scp_mock(sys.argv[2], sys.argv[3])

    if sys.argv[1] == 'list':
        return list_mock(sys.argv[2])

    return 1


if __name__ == '__main__':
    exit(main())
