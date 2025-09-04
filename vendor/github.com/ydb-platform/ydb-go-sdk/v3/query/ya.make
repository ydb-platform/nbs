GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    client.go
    execute_options.go
    result.go
    session.go
    transaction.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
