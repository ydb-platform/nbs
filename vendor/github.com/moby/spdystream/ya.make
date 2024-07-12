GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    connection.go
    handlers.go
    priority.go
    stream.go
    utils.go
)

GO_TEST_SRCS(
    priority_test.go
    spdy_bench_test.go
    spdy_test.go
)

END()

RECURSE(
    gotest
    spdy
)
