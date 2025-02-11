GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    context.go
    coordination.go
    discovery.go
    driver.go
    field.go
    level.go
    logger.go
    options.go
    ratelimiter.go
    retry.go
    scheme.go
    scripting.go
    sql.go
    table.go
    topic.go
)

GO_TEST_SRCS(
    context_test.go
    field_test.go
    logger_test.go
)

END()

RECURSE(
    gotest
)
