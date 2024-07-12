GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ua.go
    version.go
)

GO_TEST_SRCS(ua_test.go)

END()

RECURSE(
    gotest
)
