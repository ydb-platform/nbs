GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    connection.go
    context.go
    driver.go
    errors.go
    meta.go
    options.go
    sql.go
    sql_unwrap_go1.18.go
    version.go
    with.go
)

GO_TEST_SRCS(with_test.go)

GO_XTEST_SRCS(
    # example_test.go
    query_bind_test.go
)

END()

RECURSE(
    balancers
    config
    coordination
    credentials
    discovery
    gotest
    internal
    log
    meta
    metrics
    ratelimiter
    retry
    scheme
    scripting
    sugar
    table
    testutil
    topic
    trace
)
