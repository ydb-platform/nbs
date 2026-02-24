GO_LIBRARY()

GO_TEST_SRCS(
    traversal_test.go
)

SRCS(
    traversal.go
)

PEERDIR(
    cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/filesystem_traversal/config
)

END()

RECURSE(
    config
    storage
)

RECURSE_FOR_TESTS(
    tests
)
