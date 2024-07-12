GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    error.go
    parser.go
)

GO_TEST_SRCS(
    error_test.go
    parser_test.go
)

END()

RECURSE(
    gotest
)
