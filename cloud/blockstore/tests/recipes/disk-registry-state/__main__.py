import yatest.common as yatest_common

from yatest.common import process
from library.python.testing.recipe import declare_recipe

BACKUP_PATH = "cloud/blockstore/tests/recipes/disk-registry-state/data/backup.json"
GENERATOR_PATH = "cloud/blockstore/tools/testing/disk-registry-state-generator/disk-registry-state-generator"


def start(argv):
    backup_path = yatest_common.source_path(BACKUP_PATH)
    process.execute([
        yatest_common.binary_path(GENERATOR_PATH),
        "--backup-path",
        backup_path,
    ])


def stop(argv):
    None


if __name__ == "__main__":
    declare_recipe(start, stop)
