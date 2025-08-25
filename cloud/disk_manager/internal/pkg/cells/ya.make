GO_LIBRARY()

SRCS(
    interface.go
    cells.go
    get_cluster_capacity_task.go
)

GO_TEST_SRCS(
    cells_test.go
)

END()

RECURSE(
    config
    protos
    storage
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
