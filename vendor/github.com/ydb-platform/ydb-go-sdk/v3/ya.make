GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

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
    params_map.go
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
    example_test.go
    params_test.go
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
    operation
    query
    ratelimiter
    retry
    scheme
    scripting
    spans
    sugar
    table
    testutil
    topic
    trace
)
