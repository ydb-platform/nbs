import argparse
import os

import contrib.ydb.tests.library.common.yatest_common as yatest_common
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
from library.python.testing.recipe import declare_recipe, set_env

from cloud.disk_manager.test.recipe.access_service_launcher import AccessServiceLauncher
from cloud.disk_manager.test.recipe.compute_launcher import ComputeLauncher
from cloud.disk_manager.test.recipe.disk_manager_launcher import DiskManagerLauncher
from cloud.disk_manager.test.recipe.kikimr_launcher import KikimrLauncher
from cloud.disk_manager.test.recipe.kms_launcher import KmsLauncher
from cloud.disk_manager.test.recipe.metadata_service_launcher import MetadataServiceLauncher
from cloud.disk_manager.test.recipe.nbs_launcher import NbsLauncher
from cloud.disk_manager.test.recipe.nfs_launcher import NfsLauncher

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
    parser.add_argument("--kikimr-only", action=argparse.BooleanOptionalAction)
    parser.add_argument("--encryption", action=argparse.BooleanOptionalAction)
    parser.add_argument("--multiple-nbs", action=argparse.BooleanOptionalAction)
    parser.add_argument("--nbs-only", action=argparse.BooleanOptionalAction)
    parser.add_argument("--nfs-only", action=argparse.BooleanOptionalAction)
    parser.add_argument("--multiple-disk-managers", action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "--disk-manager-binary-path",
        type=str,
        default="cloud/disk_manager/cmd/disk-manager/disk-manager")

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

    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")
    nbs_binary_path = yatest_common.binary_path("cloud/nbs_internal/blockstore/daemon/blockstore-server")
    nfs_binary_path = yatest_common.binary_path("cloud/nbs_internal/filestore/server/filestore-server")
    disk_manager_binary_path = yatest_common.binary_path(args.disk_manager_binary_path)

    kikimr = KikimrLauncher(kikimr_binary_path=kikimr_binary_path)
    kikimr.start()
    set_env("DISK_MANAGER_RECIPE_KIKIMR_PORT", str(kikimr.port))

    if args.kikimr_only:
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

    nbs = NbsLauncher(
        kikimr.port,
        kikimr.domains_txt,
        kikimr.dynamic_storage_pools,
        root_certs_file,
        cert_file,
        cert_key_file,
        kikimr_binary_path=kikimr_binary_path,
        nbs_binary_path=nbs_binary_path,
        kikimr_client=kikimr.client,
        compute_port=compute_port,
        kms_port=kms_port)
    nbs.start()
    set_env("DISK_MANAGER_RECIPE_NBS_PORT", str(nbs.port))

    if args.multiple_nbs:
        kikimr2 = KikimrLauncher(kikimr_binary_path=kikimr_binary_path)
        kikimr2.start()

        nbs2 = NbsLauncher(
            kikimr2.port,
            kikimr2.domains_txt,
            kikimr2.dynamic_storage_pools,
            root_certs_file,
            cert_file,
            cert_key_file,
            kikimr_binary_path=kikimr_binary_path,
            nbs_binary_path=nbs_binary_path,
            kikimr_client=kikimr2.client,
            compute_port=compute_port,
            kms_port=kms_port)
        nbs2.start()

        kikimr3 = KikimrLauncher(kikimr_binary_path=kikimr_binary_path)
        kikimr3.start()

        nbs3 = NbsLauncher(
            kikimr3.port,
            kikimr3.domains_txt,
            kikimr3.dynamic_storage_pools,
            root_certs_file,
            cert_file,
            cert_key_file,
            kikimr_binary_path=kikimr_binary_path,
            nbs_binary_path=nbs_binary_path,
            kikimr_client=kikimr3.client,
            compute_port=compute_port,
            kms_port=kms_port)
        nbs3.start()
    else:
        nbs2 = nbs
        nbs3 = nbs
    set_env("DISK_MANAGER_RECIPE_NBS2_PORT", str(nbs2.port))
    set_env("DISK_MANAGER_RECIPE_NBS3_PORT", str(nbs3.port))

    if args.nbs_only:
        return

    nfs = NfsLauncher(
        kikimr_port=kikimr.port,
        domains_txt=kikimr.domains_txt,
        names_txt=kikimr.names_txt,
        nfs_binary_path=nfs_binary_path)
    nfs.start()
    set_env("DISK_MANAGER_RECIPE_NFS_PORT", str(nfs.port))

    if args.nfs_only:
        return

    metadata_service = MetadataServiceLauncher()
    metadata_service.start()

    access_service = AccessServiceLauncher(cert_file, cert_key_file)
    access_service.start()
    set_env("DISK_MANAGER_RECIPE_ACCESS_SERVICE_PORT", str(access_service.port))

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
            kikimr_port=kikimr.port,
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
            access_service_port=access_service.port,
            cert_file=cert_file,
            cert_key_file=cert_key_file,
        )
        disk_managers.append(disk_manager)
        disk_manager.start()

    dataplane_disk_managers_count = 1
    for _ in range(0, dataplane_disk_managers_count):
        idx = len(disk_managers)
        disk_manager = DiskManagerLauncher(
            hostname="localhost{}".format(idx),
            kikimr_port=kikimr.port,
            nbs_port=nbs.port,
            nbs2_port=nbs2.port,
            nbs3_port=nbs3.port,
            metadata_url=metadata_service.url,
            root_certs_file=root_certs_file,
            idx=idx,
            is_dataplane=True,
            disk_manager_binary_path=disk_manager_binary_path,
            with_nemesis=args.nemesis,
            s3_port=int(os.getenv("S3MDS_PORT")),
            s3_credentials_file=s3_credentials_file
        )
        disk_managers.append(disk_manager)
        disk_manager.start()

    # First node is always control plane.
    set_env("DISK_MANAGER_RECIPE_DISK_MANAGER_PORT", str(disk_managers[0].port))
    set_env("DISK_MANAGER_RECIPE_SERVER_CONFIG", disk_managers[0].server_config)


def stop(argv):
    DiskManagerLauncher.stop()
    AccessServiceLauncher.stop()
    MetadataServiceLauncher.stop()
    NfsLauncher.stop()
    NbsLauncher.stop()
    KmsLauncher.stop()
    ComputeLauncher.stop()
    KikimrLauncher.stop()


if __name__ == "__main__":
    declare_recipe(start, stop)
