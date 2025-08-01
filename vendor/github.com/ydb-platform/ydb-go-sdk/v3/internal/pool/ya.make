GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    defaults.go
    errors.go
    pool.go
    trace.go
)

GO_TEST_SRCS(pool_test.go)

END()

RECURSE(
    gotest
    stats
)
