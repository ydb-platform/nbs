DEPENDS(
    cloud/filestore/tools/testing/fs_posix_compliance/suite
    cloud/filestore/tools/testing/fs_posix_compliance/suite/bin
)

DATA(
    arcadia/cloud/filestore/tools/testing/fs_posix_compliance/suite/tests
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/filestore/tools/testing/fs_posix_compliance/pylib
    cloud/filestore/tools/testing/fs_posix_compliance/suite/python_tests
)
