GO_LIBRARY()

SRCS(
    common.go
    disks.go
    disk_cleanup.go
    filesystems.go
    filesystem_snapshots.go
    images.go
    placement_groups.go
    snapshots.go
    storage.go
)

GO_TEST_SRCS(
    common_test.go
    disks_test.go
    disk_deletion_tracking_test.go
    filesystems_test.go
    filesystem_snapshots_test.go
    images_test.go
    placement_groups_test.go
    snapshots_test.go
)

END()

RECURSE_FOR_TESTS(
    mocks
    tests
)
