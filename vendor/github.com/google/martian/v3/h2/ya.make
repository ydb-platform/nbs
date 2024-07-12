GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    h2.go
    processor.go
    queued_frames.go
    relay.go
)

GO_XTEST_SRCS(h2_test.go)

END()

RECURSE(
    gotest
    grpc
    testing
    testservice
)
