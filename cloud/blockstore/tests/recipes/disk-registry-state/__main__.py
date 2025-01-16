import os
import random
import string
import yatest.common as yatest_common

from library.python.testing.recipe import declare_recipe, set_env
from yatest.common import process


SUB_DIR = "disk-registry"
GENERATOR_PATH = "cloud/blockstore/tools/testing/disk-registry-state-generator/disk-registry-state-generator"


def start(argv):
    working_dir = os.path.join(yatest_common.output_path(), SUB_DIR)
    os.makedirs(working_dir, exist_ok=True)
    backup_file = ''.join(random.choices(string.ascii_letters, k=6)) + ".json"
    backup_path = os.path.join(working_dir, backup_file)

    process.execute([
        yatest_common.binary_path(GENERATOR_PATH),
        "--backup-path", backup_path,
    ])

    set_env("DISK_REGISTRY_BACKUP_PATH", backup_path)


def stop(argv):
    None


if __name__ == "__main__":
    declare_recipe(start, stop)
