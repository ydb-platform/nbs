GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    httpspec.go
)

GO_TEST_SRCS(httpspec_test.go)

END()

RECURSE(
    gotest
)
