GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    call.go
    coordination.go
    coordination_gtrace.go
    details.go
    discovery.go
    discovery_gtrace.go
    driver.go
    driver_gtrace.go
    ratelimiter.go
    ratelimiter_gtrace.go
    retry.go
    retry_gtrace.go
    scheme.go
    scheme_gtrace.go
    scripting.go
    scripting_gtrace.go
    sql.go
    sql_gtrace.go
    table.go
    table_gtrace.go
    topic.go
    topic_gtrace.go
    traceutil.go
)

GO_TEST_SRCS(
    details_test.go
    trace_test.go
)

END()

RECURSE(
    gotest
)
