import argparse
import datetime
import json
import logging
import os
import random
import time

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.common import shutdown

from library.python.testing.recipe import declare_recipe
import yatest.common as common

process = None

logger = logging.getLogger(__name__)

PID_FILE_NAME = "tablets_restarter_recipe.pid"


def get_client():
    port = os.getenv("NFS_SERVER_PORT")
    if port is None:
        raise ValueError("NFS_SERVER_PORT is not set")
    binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
    return FilestoreCliClient(binary_path, port, cwd=common.output_path())


def restart_tablets(client: FilestoreCliClient, seed: int):
    response = client.execute_action("listlocalfilestores", {})
    local_filestores = json.loads(response)["FileSystemIds"]

    logger.info(f"Local filestores: {local_filestores}, seed: {seed}")
    random.seed(seed)
    filestores_to_restart = [fs for fs in local_filestores if random.randint(0, 1) == 0]
    logger.info(f"Filestores to restart: {filestores_to_restart}")
    for fs in filestores_to_restart:
        logger.info(f"Restarting filestore {fs}")
        client.execute_action("restarttablet", {"FileSystemId": fs})


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--restart-interval', help='restart the process every N seconds', type=int, default=5)
    args = parser.parse_args(argv)

    client = get_client()

    start_ts = datetime.datetime.now()
    interval = datetime.timedelta(seconds=args.restart_interval)

    pid = os.fork()
    if pid:
        with open(PID_FILE_NAME, "w") as f:
            f.write(str(pid))
            logger.info(f"Started tablets restarter process with PID {pid}")
            exit()

    os.setsid()

    restarter_log_name = os.path.join(common.output_path(), "tablets_restarter.log")
    logfile = open(restarter_log_name, "w")
    if logfile is None:
        raise ValueError("Could not open log file")
    os.dup2(logfile.fileno(), os.sys.stdout.fileno())
    os.dup2(logfile.fileno(), os.sys.stderr.fileno())

    while True:
        now = datetime.datetime.now()
        deadline = start_ts + interval
        if deadline is None or now >= deadline:
            start_ts = now

            seed = random.randint(0, 1000000)
            logger.info(f"Restarting tablets with seed {seed}")
            restart_tablets(client, seed)
        else:
            time.sleep(min((deadline - now).seconds, 1))


def stop(argv):
    if not os.path.exists(PID_FILE_NAME):
        return

    with open(PID_FILE_NAME) as f:
        pid = int(f.read())
        shutdown(pid)


if __name__ == '__main__':
    declare_recipe(start, stop)
