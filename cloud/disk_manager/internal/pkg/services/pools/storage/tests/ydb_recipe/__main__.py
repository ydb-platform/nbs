import os

from cloud.tasks.test.common.processes import kill_processes, register_process
from contrib.ydb.tests.library.common import yatest_common
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from library.python.testing.recipe import declare_recipe, set_env
from yatest_lib.ya import TestMisconfigurationException


def start(argv):
    try:
        ydb_binary = yatest_common.binary_path(
            "cloud/storage/core/tools/testing/ydb/bin/ydbd"
        )
    except TestMisconfigurationException:
        ydb_binary = None

    if ydb_binary is None:
        ydb_binary = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    # Match the existing DM YDB launcher, while avoiding the full service recipe.
    config = KikimrConfigGenerator(
        binary_paths=[ydb_binary],
        erasure=None,
        static_pdisk_size=64 * 2**30,
        dynamic_storage_pools=[
            dict(name=f"dynamic_storage_pool:{i}", kind=kind, pdisk_user_kind=0)
            for i, kind in enumerate(("rot", "ssd", "rotencrypted", "ssdencrypted"), 1)
        ],
        use_in_memory_pdisks=True,
        enable_public_api_external_blobs=True,
    )
    os.environ["YDB_ALLOCATE_PGWIRE_PORT"] = "true"
    ydb = kikimr_cluster_factory(configurator=config)
    ydb.start()
    for node in ydb.nodes.values():
        register_process("ydb", node.pid)
    set_env("DISK_MANAGER_RECIPE_YDB_PORT", str(next(iter(ydb.nodes.values())).port))


def stop(argv):
    kill_processes("ydb")


if __name__ == "__main__":
    declare_recipe(start, stop)
