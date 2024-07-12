GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    encoder.go
)

GO_TEST_SRCS(encoder_test.go)

END()

RECURSE(
    gotest
)
