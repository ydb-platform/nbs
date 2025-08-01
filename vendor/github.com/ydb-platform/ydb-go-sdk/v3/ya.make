GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    connection.go
    context.go
    driver.go
    driver_string.go
    dsn.go
    errors.go
    meta.go
    options.go
    params_builder.go
    sql.go
    sql_unwrap.go
    version.go
    with.go
)

GO_TEST_SRCS(
    driver_string_test.go
    dsn_test.go
    with_test.go
)

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
    query
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
