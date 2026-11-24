import argparse
import json
import os

import yatest.common as common

from library.python.testing.recipe import declare_recipe, set_env

from cloud.storage.core.tests.ydb import (
    BINARY_PATH,
    PDISK_SIZE,
    Ydb,
)

USERS_FILE_NAME = "ydb_recipe.users"
YDB_FILE_NAME = "ydb_recipe.ydb"


def save_users(users):
    with open(USERS_FILE_NAME, "w") as f:
        json.dump(users, f)


def load_users():
    with open(USERS_FILE_NAME) as f:
        return json.load(f)


def start(argv):
    # A test may include several recipes that use ydb, they share one.
    if os.path.exists(USERS_FILE_NAME):
        users = load_users()
        if users["argv"] != argv:
            raise RuntimeError(
                "ydb recipe is used with different args: {} and {}".format(
                    users["argv"],
                    argv))
        users["count"] += 1
        save_users(users)
        return

    parser = argparse.ArgumentParser()
    parser.add_argument("--binary-path", action="store", default=BINARY_PATH)
    parser.add_argument(
        "--dynamic-storage-pool-kinds",
        nargs="+",
        default=["rot", "ssd"])
    parser.add_argument(
        "--static-pdisk-size",
        type=int,
        default=PDISK_SIZE)
    parser.add_argument("--enable-external-data-channel", action="store_true", default=False)
    parser.add_argument("--bs-cache-file-path", action="store", default=None)
    args = parser.parse_args(argv)

    dynamic_storage_pools = []
    for kind in args.dynamic_storage_pool_kinds:
        name = f"dynamic_storage_pool:{len(dynamic_storage_pools) + 1}"
        pool = dict(name=name, kind=kind, pdisk_user_kind=0)
        dynamic_storage_pools.append(pool)

    ydb = Ydb.start(
        binary_path=common.binary_path(args.binary_path),
        dynamic_storage_pools=dynamic_storage_pools,
        static_pdisk_size=args.static_pdisk_size,
        output_path=common.output_path("kikimr_working_dir"),
        enable_external_data_channel=args.enable_external_data_channel,
        bs_cache_file_path=args.bs_cache_file_path)

    ydb_file = os.path.abspath(YDB_FILE_NAME)
    ydb.save(ydb_file)
    save_users(dict(argv=argv, count=1))

    set_env("YDB_ROOT", ydb.domains_txt.Domain[0].Name)
    set_env("YDB_SERVER_PORT", str(ydb.port))
    set_env("YDB_FILE", ydb_file)


def stop(argv):
    if not os.path.exists(USERS_FILE_NAME):
        return

    users = load_users()
    users["count"] -= 1
    if users["count"] > 0:
        save_users(users)
        return
    os.remove(USERS_FILE_NAME)

    ydb = Ydb.load(YDB_FILE_NAME)
    ydb.stop()


if __name__ == "__main__":
    declare_recipe(start, stop)
