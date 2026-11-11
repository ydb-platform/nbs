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
    tasks.go
)

GO_TEST_SRCS(
)

END()

RECURSE_FOR_TESTS(
    tests
)
