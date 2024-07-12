GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    compression.go
)

GO_TEST_SRCS(compression_test.go)

END()

RECURSE(
    gotest
)
