GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    endpoint.go
    mux.go
    muxfunc.go
)

GO_TEST_SRCS(mux_test.go)

END()

RECURSE(
    gotest
)
