GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    parse.go
)

GO_TEST_SRCS(parse_test.go)

END()

RECURSE(
    gotest
    test
)
