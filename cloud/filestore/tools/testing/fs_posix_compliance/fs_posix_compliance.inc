INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tools/testing/fs_posix_compliance/fs_posix_compliance_suite.inc)

# The suite's own python tests (suite/python_tests): they run in every test
# that includes this file.
PEERDIR(
    cloud/filestore/tools/testing/fs_posix_compliance/suite/python_tests
)
