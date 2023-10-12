import os

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
from ydb.tests.library.harness.param_constants import kikimr_driver_path
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


def start(argv):
    certs_dir = yatest_common.source_path("cloud/blockstore/tests/certs")
    root_certs_file = os.path.join(certs_dir, "server.crt")
    cert_file = os.path.join(certs_dir, "server.crt")
    cert_key_file = os.path.join(certs_dir, "server.key")
    set_env("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE", root_certs_file)

    kikimr_binary_path = kikimr_driver_path()
    nbs_binary_path = yatest_common.binary_path("cloud/nbs_internal/blockstore/daemon/blockstore-server")
    nfs_binary_path = yatest_common.binary_path("cloud/filestore/server/filestore-server")

    if 'stable' in argv:
        kikimr_binary_path = yatest_common.build_path(
            "kikimr/public/tools/package/stable/Berkanavt/kikimr/bin/kikimr")
        nbs_binary_path = yatest_common.build_path(
            "cloud/blockstore/tests/recipes/local-kikimr/stable-package-nbs/usr/bin/blockstore-server")

    with_nemesis = 'nemesis' in argv

    kikimr = KikimrLauncher(kikimr_binary_path=kikimr_binary_path)
    kikimr.start()
    set_env("DISK_MANAGER_RECIPE_KIKIMR_PORT", str(kikimr.port))

    if 'kikimr' in argv:
        return

    compute_port = 0,
    kms_port = 0

    if 'encryption' in argv:
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

    if 'multiple-nbs' in argv:
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
    else:
        nbs2 = nbs
    set_env("DISK_MANAGER_RECIPE_NBS2_PORT", str(nbs2.port))

    if 'nbs' in argv:
        return

    nfs = NfsLauncher(
        kikimr_port=kikimr.port,
        domains_txt=kikimr.domains_txt,
        names_txt=kikimr.names_txt,
        nfs_binary_path=nfs_binary_path)
    nfs.start()
    set_env("DISK_MANAGER_RECIPE_NFS_PORT", str(nfs.port))

    if 'nfs' in argv:
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

    controlplane_disk_manager_count = 2 if 'multiple-disk-managers' in argv else 1
    for _ in range(0, controlplane_disk_manager_count):
        idx = len(disk_managers)
        disk_manager = DiskManagerLauncher(
            hostname="localhost{}".format(idx),
            kikimr_port=kikimr.port,
            nbs_port=nbs.port,
            nbs2_port=nbs2.port,
            metadata_url=metadata_service.url,
            root_certs_file=root_certs_file,
            idx=idx,
            is_dataplane=False,
            with_nemesis=with_nemesis,
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
            metadata_url=metadata_service.url,
            root_certs_file=root_certs_file,
            idx=idx,
            is_dataplane=True,
            with_nemesis=with_nemesis,
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
