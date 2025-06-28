GO_LIBRARY()

SRCS(
    interface.go
    cells.go
)

GO_TEST_SRCS(
    cells_test.go
)

END()

RECURSE(
    config
    storage
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
