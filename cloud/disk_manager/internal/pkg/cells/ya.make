GO_LIBRARY()

SRCS(
    interface.go
    cells.go
)

GO_TEST_SRCS(
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    mocks
)
