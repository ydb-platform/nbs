import os

import yatest.common as common
import yatest.common.network as network

from cloud.blockstore.tests.python.lib.fake_root_kms import start_fake_root_kms
from cloud.tasks.test.common.processes import register_process, kill_processes

from library.python.testing.recipe import declare_recipe, set_env


SERVICE_NAME = "fake_root_kms"


def start(argv):

    pm = network.PortManager()
    port = pm.get_port()

    try:
        test_name = common.context.test_name or ''
        test_name = test_name.translate(str.maketrans({':': '_', '/': '_'}))
    except AttributeError:
        test_name = ''

    working_dir = os.path.join(common.output_path(), test_name, "root_kms")
    os.makedirs(working_dir, exist_ok=True)

    root_kms, environment = start_fake_root_kms(working_dir, port)
    register_process(SERVICE_NAME, root_kms.process.pid)

    for name, value in environment.items():
        set_env(name, value)


def stop(argv):
    kill_processes(SERVICE_NAME)


if __name__ == "__main__":
    declare_recipe(start, stop)
