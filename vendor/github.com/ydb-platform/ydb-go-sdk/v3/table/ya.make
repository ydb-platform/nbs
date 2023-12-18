GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    table.go
)

GO_XTEST_SRCS(
    example_test.go
    table_test.go
)

END()

RECURSE(
    gotest
    options
    result
    stats
    types
)
