GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    alignedbuff.go
)

GO_TEST_SRCS(alignedbuff_test.go)

END()

RECURSE(
    gotest
)
