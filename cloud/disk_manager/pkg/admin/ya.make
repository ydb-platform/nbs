GO_LIBRARY()

SRCS(
    common.go
    disks.go
    filesystem.go
    filesystem_snapshots.go
    images.go
    operations.go
    placement_group.go
    pools.go
    private.go
    run.go
    snapshots.go
    snapshots_export_source.go
    snapshots_export_s3.go
    tasks.go
)

GO_TEST_SRCS(
    snapshots_export_source_test.go
    snapshots_export_s3_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
