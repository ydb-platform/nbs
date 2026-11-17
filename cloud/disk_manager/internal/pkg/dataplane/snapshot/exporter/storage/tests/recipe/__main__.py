import yatest.common

from cloud.disk_manager.test.recipe.ydb_launcher import YDBLauncher
from library.python.testing.recipe import declare_recipe, set_env


def start(argv):
    launcher = YDBLauncher(yatest.common.binary_path("cloud/storage/core/tools/testing/ydb/bin/ydbd"))
    launcher.start()
    set_env("DISK_MANAGER_RECIPE_YDB_PORT", str(launcher.port))


def stop(argv):
    YDBLauncher.stop()


if __name__ == "__main__":
    declare_recipe(start, stop)
