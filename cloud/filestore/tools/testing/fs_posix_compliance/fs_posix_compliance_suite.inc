# The compliance suites (suite/tests) and everything needed to run them, but
# not the suite's own python tests (suite/python_tests): to run the suites as a
# workload.

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
)
