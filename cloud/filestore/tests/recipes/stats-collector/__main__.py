import argparse
import datetime
import logging
import re
import os
import time

from cloud.filestore.tests.python.lib.common import shutdown

from library.python.testing.recipe import declare_recipe
import yatest.common as common

process = None

logger = logging.getLogger(__name__)

PID_FILE_NAME = "stats_collector_recipe.pid"


def collect_stats(logger, endpoint):
    try:
        import requests
    except ImportError:
        logger.error(
            "requests module is not installed. Please install it to use the stats collector."
        )
        return

    try:
        response = requests.get(endpoint)
        logger.info(f"Stats collected from {endpoint}: {response.status_code}")
        if response.status_code == 200:
            logger.info(f"Response: {response.text}")
        else:
            logger.error(f"Failed to collect stats from {endpoint}")
    except Exception as e:
        logger.error(f"Error collecting stats from {endpoint}: {e}")


def expand_vars(s, env):
    # Pattern matches $VAR or ${VAR}
    pattern = re.compile(r'\$(\w+)|\$\{([^}]+)\}')

    def replacer(match):
        var1, var2 = match.groups()
        var_name = var1 or var2
        return env.get(var_name, '')

    return pattern.sub(replacer, s)


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--period", type=int, help="Period in seconds to collect stats"
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        help="Endpoint to collect stats from. Note that it can contain env variables, so it will be expanded using os.environ",
        required=True,
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file for the stats collector log",
        default="stats_collector.log",
    )
    args = parser.parse_args(argv)

    start_ts = datetime.datetime.now()
    interval = datetime.timedelta(seconds=args.period)
    endpoint = expand_vars(args.endpoint, os.environ)

    pid = os.fork()
    if pid:
        with open(PID_FILE_NAME, "w") as f:
            f.write(str(pid))
            logger.info(f"Started stats collector process with PID {pid}")
            exit()

    os.setsid()

    collector_log_name = os.path.join(common.output_path(), args.output)
    logfile = open(collector_log_name, "w")
    if logfile is None:
        raise ValueError(f"Could not open log file: {collector_log_name}")
    os.dup2(logfile.fileno(), os.sys.stdout.fileno())
    os.dup2(logfile.fileno(), os.sys.stderr.fileno())

    while True:
        now = datetime.datetime.now()
        deadline = start_ts + interval
        if deadline is None or now >= deadline:
            start_ts = now

            collect_stats(logger, endpoint)
        else:
            time.sleep(min((deadline - now).seconds, 1))


def stop(argv):
    if not os.path.exists(PID_FILE_NAME):
        return

    with open(PID_FILE_NAME) as f:
        pid = int(f.read())
        shutdown(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
