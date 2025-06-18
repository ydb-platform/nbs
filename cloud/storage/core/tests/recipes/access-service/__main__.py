import os
import logging
import signal
import argparse

import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.storage.core.tools.testing.access_service.lib import AccessService
from library.python.testing.recipe import declare_recipe, set_env
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

logger = logging.getLogger(__name__)
ERR_LOG_FILE_NAMES_FILE = "access_service_recipe.err_log_files"

def start(argv):
    args = _parse_args(argv)
    start_access_service(args)


def start_access_service(args):
    logger.info("start access-service")

    port_manager = yatest_common.PortManager()
    port = port_manager.get_port()
    control_server_port = port_manager.get_port()

    access_service = AccessService(
        host="localhost",
        port=port,
        control_server_port=control_server_port)

    access_service.start()

    append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, access_service.daemon.err_log_file)

    set_env("ACCESS_SERVICE_PID", str(access_service.pid))
    set_env("ACCESS_SERVICE_PORT", str(port))
    set_env("ACCESS_SERVICE_CONTROL_PORT", str(control_server_port))

    if args.token is not None:
        access_service.authenticate(args.token)
        access_service.authorize(args.token)
        set_env("ACCESS_SERVICE_TOKEN", args.token)


def stop(argv):
    stop_access_service()

    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError(
            "Errors occurred during access-service execution: {}".format(errors)
        )


def stop_access_service():
    logger.info("shutdown access-service")

    pid = os.getenv("ACCESS_SERVICE_PID")

    if pid:
        logger.info("will kill access-service with pid `%s`", pid)
        try:
            os.kill(int(pid), signal.SIGTERM)
        except OSError:
            logger.exception("While killing pid `%s`", pid)
            raise


def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--token", help="iam token to authorize")
    args = parser.parse_args(argv)

    return args


if __name__ == "__main__":
    declare_recipe(start, stop)
