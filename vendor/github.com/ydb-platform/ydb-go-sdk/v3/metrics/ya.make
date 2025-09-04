GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    config.go
    coordination.go
    counter.go
    discovery.go
    driver.go
    error_brief.go
    gauge.go
    histogram.go
    node_id.go
    query.go
    ratelimiter.go
    registry.go
    retry.go
    scheme.go
    scripting.go
    sql.go
    table.go
    timer.go
    traces.go
)

GO_TEST_SRCS(error_brief_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
