GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    defaults.go
    errors.go
    pool.go
    stats.go
    trace.go
)

GO_TEST_SRCS(pool_test.go)

END()

RECURSE(
    gotest
)
