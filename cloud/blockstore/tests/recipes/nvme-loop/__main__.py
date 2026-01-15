import os

from library.python.testing.recipe import declare_recipe, set_env


NVME_LOOP_DEVICES_FOLDER = "/opt/nvme-loop/devices"
NVME_LOOP_LOCKS_FOLDER = "/tmp/nvme-loop/locks"


def start(argv):

    os.makedirs(NVME_LOOP_LOCKS_FOLDER, exist_ok=True)

    set_env("NVME_LOOP_DEVICES_FOLDER", NVME_LOOP_DEVICES_FOLDER)
    set_env("NVME_LOOP_LOCKS_FOLDER", NVME_LOOP_LOCKS_FOLDER)


def stop(argv):
    pass


if __name__ == "__main__":
    declare_recipe(start, stop)
