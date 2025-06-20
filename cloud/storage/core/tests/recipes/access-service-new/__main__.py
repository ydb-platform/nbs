import argparse
import logging
import os
import signal

from library.python.testing.recipe import declare_recipe, set_env

from cloud.storage.core.tools.testing.access_service_new.lib import NewAccessService
import contrib.ydb.tests.library.common.yatest_common as yatest_common
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

logger = logging.getLogger(__name__)
ERR_LOG_FILE_NAMES_FILE = "access_service_new_recipe.err_log_files"

def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--encryption", action=argparse.BooleanOptionalAction)
    args = parser.parse_args(args=args)
    return args


def start(argv):
    args = parse_args(argv)
    logger.info("Starting new access-service")
    working_dir = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder=""
    )
    ensure_path_exists(working_dir)
    port_manager = yatest_common.PortManager()
    port = port_manager.get_port()
    control_port = port_manager.get_port()

    cert_file = ""
    cert_key_file = ""
    if args.encryption:
        certs_dir = yatest_common.source_path("cloud/blockstore/tests/certs")
        cert_file = os.path.join(certs_dir, "server.crt")
        cert_key_file = os.path.join(certs_dir, "server.key")

    access_service = NewAccessService(
        host="localhost",
        binary_path=yatest_common.binary_path(
            "cloud/storage/core/tools/testing/access_service_new/mock/access-service-mock",
        ),
        port=port,
        control_port=control_port,
        working_dir=working_dir,
        cert_file=cert_file,
        cert_key_file=cert_key_file,
    )
    access_service.start()
    append_recipe_err_files(
        ERR_LOG_FILE_NAMES_FILE, access_service.daemon.stderr_file_name
    )
    set_env("ACCESS_SERVICE_TYPE", "new")
    set_env("ACCESS_SERVICE_PORT", str(port))
    set_env("ACCESS_SERVICE_CONTROL_PORT", str(control_port))
    set_env("ACCESS_SERVICE_PID", str(access_service.pid))


def stop(argv):
    logger.info("Shutdown new access-service")

    pid = os.getenv("ACCESS_SERVICE_PID")

    logger.info("Killing access-service with pid `%s`", pid)
    try:
        os.kill(int(pid), signal.SIGTERM)
    except OSError:
        logger.exception("While killing pid `%s`", pid)
        raise

    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError(
            "Errors occurred during new access-service-new execution: {}".format(errors)
        )


if __name__ == "__main__":
    declare_recipe(start, stop)
