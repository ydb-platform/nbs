GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    addr.go
    const.go
    mux.go
    session.go
    stream.go
    util.go
)

GO_TEST_SRCS(
    bench_test.go
    const_test.go
    session_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
