import argparse
import logging
import os

import contrib.ydb.tests.library.common.yatest_common as yatest_common
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
from library.python.testing.recipe import declare_recipe, set_env

from cloud.disk_manager.test.recipe.common import get_ydb_binary_path
from cloud.disk_manager.test.recipe.compute_launcher import ComputeLauncher
from cloud.disk_manager.test.recipe.disk_manager_launcher import DiskManagerLauncher
from cloud.disk_manager.test.recipe.kms_launcher import KmsLauncher
from cloud.disk_manager.test.recipe.metadata_service_launcher import MetadataServiceLauncher
from cloud.disk_manager.test.recipe.nbs_launcher import NbsLauncher
from cloud.disk_manager.test.recipe.nfs_launcher import NfsLauncher
from cloud.disk_manager.test.recipe.s3_launcher import S3Launcher
from cloud.disk_manager.test.recipe.ydb_launcher import YDBLauncher

S3_CREDENTIALS_FILE = """
{
    "id": "test",
    "secret": "test"
}
"""


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--certs-only", action=argparse.BooleanOptionalAction)
    parser.add_argument("--nemesis", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "--min-restart-period-sec",
        action='store',
        type=int,
        default=5,
        required=False,
        dest='min_restart_period_sec'
    )
    parser.add_argument(
        "--max-restart-period-sec",
        action='store',
        type=int,
        default=30,
        required=False,
        dest='max_restart_period_sec'
    )
    parser.add_argument("--ydb-only", action=argparse.BooleanOptionalAction)
    parser.add_argument("--encryption", action=argparse.BooleanOptionalAction)
    parser.add_argument("--multiple-nbs", action=argparse.BooleanOptionalAction)
    parser.add_argument("--nbs-only", action=argparse.BooleanOptionalAction)
    parser.add_argument("--nfs-only", action=argparse.BooleanOptionalAction)
    parser.add_argument("--multiple-disk-managers", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "--disk-manager-binary-path",
        type=str,
        default="cloud/disk_manager/cmd/disk-manager/disk-manager")
    parser.add_argument("--creation-and-deletion-allowed-only-for-disks-with-id-prefix", type=str, default="")
    parser.add_argument("--disable-disk-registry-based-disks", action='store_true', default=False)
    parser.add_argument("--disk-agent-count", type=int, default=1)
    parser.add_argument("--retry-broken-drbased-disk-checkpoint", action='store_true', default=False)

    args, _ = parser.parse_known_args(args=args)
    return args


def start(argv):
    args = parse_args(argv)

    certs_dir = yatest_common.source_path("cloud/blockstore/tests/certs")
    root_certs_file = os.path.join(certs_dir, "server.crt")
    cert_file = os.path.join(certs_dir, "server.crt")
    cert_key_file = os.path.join(certs_dir, "server.key")
    set_env("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE", root_certs_file)
    set_env("DISK_MANAGER_RECIPE_CERT_FILE", cert_file)
    set_env("DISK_MANAGER_RECIPE_CERT_KEY_FILE", cert_key_file)

    if args.certs_only:
        return

    s3 = S3Launcher()
    s3.start()
    set_env("DISK_MANAGER_RECIPE_S3_PORT", str(s3.port))

    ydb_binary_path = get_ydb_binary_path()
    nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")
    disk_agent_binary_path = yatest_common.binary_path("cloud/blockstore/apps/disk_agent/diskagentd")
    nfs_binary_path = yatest_common.binary_path("cloud/filestore/apps/server/filestore-server")
    disk_manager_binary_path = yatest_common.binary_path(args.disk_manager_binary_path)

    ydb = YDBLauncher(ydb_binary_path=ydb_binary_path)
    ydb.start()
    set_env("DISK_MANAGER_RECIPE_YDB_PORT", str(ydb.port))

    if args.ydb_only:
        return

    compute_port = 0,
    kms_port = 0

    if args.encryption:
        compute = ComputeLauncher()
        compute.start()
        compute_port = compute.port

        kms = KmsLauncher()
        kms.start()
        kms_port = kms.port

    base_disk_id_prefix = "base-"
    proxy_overlay_disk_id_prefix = "proxy-"

    destruction_allowed_only_for_disks_with_id_prefixes = []
    if args.creation_and_deletion_allowed_only_for_disks_with_id_prefix:
        destruction_allowed_only_for_disks_with_id_prefixes = [args.creation_and_deletion_allowed_only_for_disks_with_id_prefix]
        destruction_allowed_only_for_disks_with_id_prefixes.append(base_disk_id_prefix)
        destruction_allowed_only_for_disks_with_id_prefixes.append(proxy_overlay_disk_id_prefix)

    nbs = NbsLauncher(
        ydb.port,
        ydb.domains_txt,
        ydb.dynamic_storage_pools,
        root_certs_file,
        cert_file,
        cert_key_file,
        ydb_binary_path=ydb_binary_path,
        nbs_binary_path=nbs_binary_path,
        disk_agent_binary_path=disk_agent_binary_path,
        ydb_client=ydb.client,
        compute_port=compute_port,
        kms_port=kms_port,
        destruction_allowed_only_for_disks_with_id_prefixes=destruction_allowed_only_for_disks_with_id_prefixes,
        disk_agent_count=args.disk_agent_count)
    nbs.start()
    set_env("DISK_MANAGER_RECIPE_NBS_PORT", str(nbs.port))

    if args.multiple_nbs:
        ydb2 = YDBLauncher(ydb_binary_path=ydb_binary_path)
        ydb2.start()

        nbs2 = NbsLauncher(
            ydb2.port,
            ydb2.domains_txt,
            ydb2.dynamic_storage_pools,
            root_certs_file,
            cert_file,
            cert_key_file,
            ydb_binary_path=ydb_binary_path,
            nbs_binary_path=nbs_binary_path,
            disk_agent_binary_path=disk_agent_binary_path,
            ydb_client=ydb2.client,
            compute_port=compute_port,
            kms_port=kms_port,
            destruction_allowed_only_for_disks_with_id_prefixes=destruction_allowed_only_for_disks_with_id_prefixes)
        nbs2.start()

        ydb3 = YDBLauncher(ydb_binary_path=ydb_binary_path)
        ydb3.start()

        nbs3 = NbsLauncher(
            ydb3.port,
            ydb3.domains_txt,
            ydb3.dynamic_storage_pools,
            root_certs_file,
            cert_file,
            cert_key_file,
            ydb_binary_path=ydb_binary_path,
            nbs_binary_path=nbs_binary_path,
            disk_agent_binary_path=disk_agent_binary_path,
            ydb_client=ydb3.client,
            compute_port=compute_port,
            kms_port=kms_port,
            destruction_allowed_only_for_disks_with_id_prefixes=destruction_allowed_only_for_disks_with_id_prefixes)
        nbs3.start()
    else:
        nbs2 = nbs
        nbs3 = nbs
    set_env("DISK_MANAGER_RECIPE_NBS2_PORT", str(nbs2.port))
    set_env("DISK_MANAGER_RECIPE_NBS3_PORT", str(nbs3.port))

    if args.nbs_only:
        return

    nfs = NfsLauncher(
        ydb_port=ydb.port,
        domains_txt=ydb.domains_txt,
        names_txt=ydb.names_txt,
        nfs_binary_path=nfs_binary_path)
    nfs.start()
    set_env("DISK_MANAGER_RECIPE_NFS_PORT", str(nfs.port))

    if args.nfs_only:
        return

    metadata_service = MetadataServiceLauncher()
    metadata_service.start()

    working_dir = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder=""
    )
    ensure_path_exists(working_dir)

    s3_credentials_file = os.path.join(working_dir, 's3_credentials.json')

    with open(s3_credentials_file, "w") as f:
        f.write(S3_CREDENTIALS_FILE)

    disk_managers = []

    controlplane_disk_manager_count = 2 if args.multiple_disk_managers else 1
    for _ in range(0, controlplane_disk_manager_count):
        idx = len(disk_managers)
        disk_manager = DiskManagerLauncher(
            hostname="localhost{}".format(idx),
            ydb_port=ydb.port,
            nbs_port=nbs.port,
            nbs2_port=nbs2.port,
            nbs3_port=nbs3.port,
            metadata_url=metadata_service.url,
            root_certs_file=root_certs_file,
            idx=idx,
            is_dataplane=False,
            disk_manager_binary_path=disk_manager_binary_path,
            with_nemesis=args.nemesis,
            nfs_port=nfs.port,
            access_service_port=os.getenv('DISK_MANAGER_RECIPE_ACCESS_SERVICE_PORT'),
            cert_file=cert_file,
            cert_key_file=cert_key_file,
            min_restart_period_sec=args.min_restart_period_sec,
            max_restart_period_sec=args.max_restart_period_sec,
            base_disk_id_prefix=base_disk_id_prefix,
            creation_and_deletion_allowed_only_for_disks_with_id_prefix=args.creation_and_deletion_allowed_only_for_disks_with_id_prefix,
            disable_disk_registry_based_disks=args.disable_disk_registry_based_disks,
            retry_broken_drbased_disk_checkpoint=args.retry_broken_drbased_disk_checkpoint,
        )
        disk_managers.append(disk_manager)
        disk_manager.start()
    set_env("DISK_MANAGER_RECIPE_DISK_MANAGER_MON_PORT", str(disk_managers[0].monitoring_port))

    dataplane_disk_managers_count = 1
    for _ in range(0, dataplane_disk_managers_count):
        idx = len(disk_managers)
        disk_manager = DiskManagerLauncher(
            hostname="localhost{}".format(idx),
            ydb_port=ydb.port,
            nbs_port=nbs.port,
            nbs2_port=nbs2.port,
            nbs3_port=nbs3.port,
            metadata_url=metadata_service.url,
            root_certs_file=root_certs_file,
            idx=idx,
            is_dataplane=True,
            disk_manager_binary_path=disk_manager_binary_path,
            with_nemesis=args.nemesis,
            s3_port=s3.port,
            s3_credentials_file=s3_credentials_file,
            min_restart_period_sec=args.min_restart_period_sec,
            max_restart_period_sec=args.max_restart_period_sec,
            proxy_overlay_disk_id_prefix=proxy_overlay_disk_id_prefix,
        )
        disk_managers.append(disk_manager)
        disk_manager.start()

    # First node is always control plane.
    set_env("DISK_MANAGER_RECIPE_DISK_MANAGER_PORT", str(disk_managers[0].port))
    set_env("DISK_MANAGER_RECIPE_SERVER_CONFIG", disk_managers[0].server_config)


def stop(argv):
    logging.info(os.system("ss -tpna"))

    DiskManagerLauncher.stop()
    MetadataServiceLauncher.stop()
    NfsLauncher.stop()
    NbsLauncher.stop()
    KmsLauncher.stop()
    ComputeLauncher.stop()
    YDBLauncher.stop()
    S3Launcher.stop()


if __name__ == "__main__":
    declare_recipe(start, stop)
