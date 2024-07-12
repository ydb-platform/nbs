GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    handshaker.go
)

GO_TEST_SRCS(handshaker_test.go)

END()

RECURSE(
    gotest
    service
    # yo
)
