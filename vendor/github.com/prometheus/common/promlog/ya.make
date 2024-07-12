GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    log.go
)

GO_TEST_SRCS(log_test.go)

END()

RECURSE(
    flag
    gotest
)
