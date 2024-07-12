GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    decoder.go
    hcl.go
    lex.go
    parse.go
)

GO_TEST_SRCS(
    decoder_test.go
    hcl_test.go
    lex_test.go
)

END()

RECURSE(
    gotest
    hcl
    json
)
