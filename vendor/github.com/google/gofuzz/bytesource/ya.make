GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bytesource.go
)

GO_TEST_SRCS(bytesource_test.go)

END()

RECURSE(
    gotest
)
