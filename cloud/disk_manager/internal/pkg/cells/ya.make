GO_LIBRARY()

SRCS(
    cells.go
    collect_cluster_capacity_task.go
    interface.go
    register.go
)

GO_TEST_SRCS(
    cells_test.go
    collect_cluster_capacity_task_test.go
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
