GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(TestParseIP)

SRCS(
    ip.go
    parse.go
)

GO_TEST_SRCS(ip_test.go)

END()

RECURSE(
    gotest
)
