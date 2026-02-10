import argparse
import json
import os
import tempfile
import uuid

import yatest.common as common

from cloud.tasks.test.common.processes import register_process, kill_processes

from library.python.testing.recipe import declare_recipe, set_env


SERVICE_NAME = "infra_device_provider"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--devices", type=str, required=True)
    args, _ = parser.parse_known_args(args=argv)

    binary_path = common.binary_path(
        "cloud/blockstore/tools/testing/infra-device-provider/server/server")

    try:
        test_name = common.context.test_name or ''
        test_name = test_name.translate(str.maketrans({':': '_', '/': '_'}))
    except AttributeError:
        test_name = ''

    working_dir = os.path.join(common.output_path(), test_name, "infra_device_provider")
    os.makedirs(working_dir, exist_ok=True)

    socket_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())
    devices_path = common.source_path(args.devices)

    infra = common.execute(
        command=[binary_path, '--socket', socket_path, '--devices', devices_path],
        cwd=working_dir,
        stdout=os.path.join(working_dir, "out.txt"),
        stderr=os.path.join(working_dir, "err.txt"),
        wait=False,
    )

    register_process(SERVICE_NAME, infra.process.pid)

    set_env("INFRA_DEVICE_PROVIDER_SOCKET", socket_path)


def stop(argv):
    kill_processes(SERVICE_NAME)


if __name__ == "__main__":
    declare_recipe(start, stop)
