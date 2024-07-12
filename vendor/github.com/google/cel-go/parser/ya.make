GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    errors.go
    helper.go
    input.go
    macro.go
    options.go
    parser.go
    unescape.go
    unparser.go
)

GO_TEST_SRCS(
    helper_test.go
    parser_test.go
    unescape_test.go
    unparser_test.go
)

END()

RECURSE(
    gen
    gotest
)
