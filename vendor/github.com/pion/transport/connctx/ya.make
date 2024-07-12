GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    connctx.go
    pipe.go
)

GO_TEST_SRCS(connctx_test.go)

END()

RECURSE(
    gotest
)
