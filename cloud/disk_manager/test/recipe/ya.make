PY3_PROGRAM()

PY_SRCS(
    __main__.py
    common.py
    compute_launcher.py
    disk_manager_launcher.py
    kms_launcher.py
    metadata_service_launcher.py
    nbs_launcher.py
    nfs_launcher.py
    s3_launcher.py
    ydb_launcher.py
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib
    cloud/filestore/tests/python/lib
    cloud/storage/core/tests/common
    cloud/tasks/test/common
    ydb/tests/library
    library/python/testing/recipe
)

END()
