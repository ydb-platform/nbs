import argparse
import datetime
import logging
import os
import random
import requests
import signal
import subprocess
import sys
import time


process = None

logger = logging.getLogger(__name__)


def sighandler(sig, frame):
    if process is not None:
        process.terminate()

    sys.exit(0)


def should_kill_process() -> bool:
    return bool(random.getrandbits(1))


def _process_wait_and_check(process, check_timeout):
    while True:
        try:
            process.wait(timeout=check_timeout)
        except subprocess.TimeoutExpired:
            logger.warning(
                f"wait for pid {process.pid} timed out after {check_timeout} seconds"
            )

            bt = subprocess.getoutput(
                f'sudo gdb --batch -p {process.pid} -ex "thread apply all bt"'
            )
            logger.warning(f"PID {process.pid}: backtrace:\n{bt}")
            continue
        break


def ping(port, path, success_codes):
    try:
        endpoint = f'http://localhost:{port}{path}'
        logging.debug(f'ping {endpoint} ...')
        r = requests.get(endpoint)
        if not success_codes:
            return True
        if str(r.status_code) in success_codes:
            return True
        logging.info(f'ping attempt has failed. Bad status code: {r.status_code}')
    except Exception as e:
        logging.info(f'ping attempt has failed: {e}')

    return False


def main():
    signal.signal(signal.SIGTERM, sighandler)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cmdline',
        help='cmdline to run; specify cmdline multiple times to alternate them after each restart',
        required=True,
        action='append')

    parser.add_argument('--restart-interval', help='restart the process every N seconds', type=int, default=5)
    parser.add_argument('--downtime', help='delay before the start of the process in seconds', type=int, default=0)
    parser.add_argument('--ping-port', help='ping this port via http after launching the process', type=int)
    parser.add_argument('--ping-attempts', help='the number of ping attempts before failing (0 - infinite)', type=int, default=0)
    parser.add_argument('--ping-timeout', help='the timeout between ping attempts in seconds', type=int, default=1)
    parser.add_argument('--ping-path', help='part of ping endpoint', type=str, default='')
    parser.add_argument('--ping-success-codes', help='', nargs='*')
    parser.add_argument('--allow-restart-flag', help='file to look for before restart', type=str, default=None)
    parser.add_argument('-v', '--verbose', help='verbose mode', default=0, action='count')
    parser.add_argument('--terminate-check-timeout', help='the timeout in seconds between wait attempts for terminated process', type=int, default=60)

    args = parser.parse_args()

    if args.verbose:
        log_level = max(0, logging.ERROR - 10 * int(args.verbose))
    else:
        log_level = logging.INFO

    logging.basicConfig(stream=sys.stderr, level=log_level, format="[%(levelname)s] [%(asctime)s] %(message)s")

    step = 0
    cmdline = None
    cmdline_cnt = len(args.cmdline)

    start_ts = None
    interval = datetime.timedelta(seconds=args.restart_interval)

    global process

    while True:
        now = datetime.datetime.now()
        deadline = start_ts + interval if start_ts is not None else None
        if deadline is None or now >= deadline:
            if process is not None:
                while args.allow_restart_flag is not None and not os.path.exists(args.allow_restart_flag):
                    logging.debug("waiting for the allow restart flag")
                    time.sleep(1)
                    continue

                if should_kill_process():
                    logging.info(f'killing process {cmdline}')
                    process.kill()
                else:
                    logging.info(f'terminating process {cmdline}')
                    process.terminate()
                    _process_wait_and_check(process,
                                            check_timeout=args.terminate_check_timeout)

            def start_process():
                if args.downtime > 0:
                    logging.info(f'waiting {args.downtime} seconds before starting process')
                    time.sleep(args.downtime)

                logging.info(f'starting process {cmdline}')
                return subprocess.Popen(cmdline.split())

            cmdline = args.cmdline[step % cmdline_cnt]
            step += 1

            process = start_process()

            if args.ping_port is not None:
                attempts = 0
                success = False
                while attempts < args.ping_attempts or args.ping_attempts == 0:
                    if ping(args.ping_port, args.ping_path, args.ping_success_codes):
                        success = True
                        break

                    if process.poll() is not None:
                        if process.poll() in [0, 1, 100]:
                            logging.info(f'subprocess failed to start, code {process.poll()}')
                            logging.info(os.system("ss -tpna"))
                            logging.info(os.system("ps aux"))
                            process = start_process()
                        else:
                            logging.fatal(f'unexpected exit code {process.poll()}')
                            return 1

                    time.sleep(args.ping_timeout)
                    attempts += 1

                if not success:
                    logging.fatal('all ping attempts have failed')
                    return 2

            start_ts = datetime.datetime.now()
            logging.info(f'subprocess started at {start_ts}')
        else:
            if process.poll() is not None:
                logging.error(f'subprocess unexpectedly exited with code {process.poll()}')
                return 3

            logging.debug('sleeping')

            time.sleep(min((deadline - now).seconds, 1))

    return 0


if __name__ == '__main__':
    sys.exit(main())
