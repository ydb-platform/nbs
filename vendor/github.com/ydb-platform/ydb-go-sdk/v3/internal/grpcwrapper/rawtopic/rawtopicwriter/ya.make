GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    messages.go
    streamwriter.go
)

GO_TEST_SRCS(streamwriter_test.go)

END()

RECURSE(
    gotest
)
