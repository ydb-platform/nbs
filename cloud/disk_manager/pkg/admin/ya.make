GO_LIBRARY()

SRCS(
    common.go
    disks.go
    export.go
    export_s3.go
    export_source.go
    filesystem.go
    filesystem_snapshots.go
    images.go
    images_export.go
    operations.go
    placement_group.go
    pools.go
    private.go
    run.go
    snapshots.go
    snapshots_export_task.go
    tasks.go
)

GO_TEST_SRCS(
    images_export_test.go
    snapshots_export_s3_test.go
    snapshots_export_source_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
