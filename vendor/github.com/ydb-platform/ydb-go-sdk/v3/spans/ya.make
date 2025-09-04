GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    coordination.go
    discovery.go
    driver.go
    errors.go
    field.go
    helpers.go
    query.go
    ratelimiter.go
    retry.go
    safe.go
    scheme.go
    scripting.go
    spans.go
    sql.go
    table.go
    traces.go
)

GO_TEST_SRCS(driver_test.go)

END()

RECURSE(
    gotest
)
