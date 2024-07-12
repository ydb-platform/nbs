GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    flatten.go
    parser.go
)

GO_TEST_SRCS(parser_test.go)

END()

RECURSE(
    gotest
)
