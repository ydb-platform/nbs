OWNER(g:cloud-nbs)

PY3_PROGRAM()

PY_SRCS(
    __main__.py
    access_service_launcher.py
    compute_launcher.py
    disk_manager_launcher.py
    kikimr_launcher.py
    kms_launcher.py
    metadata_service_launcher.py
    nbs_launcher.py
    nfs_launcher.py
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/tests/python/lib
    cloud/disk_manager/test/common
    cloud/filestore/tests/python/lib
    kikimr/ci/libraries
    library/python/testing/recipe
)

END()
