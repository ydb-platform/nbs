GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    compressionType.go
)

GO_TEST_SRCS(compressionType_test.go)

END()

RECURSE(
    gotest
)
