GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    ast.go
    didyoumean.go
    doc.go
    is.go
    navigation.go
    parser.go
    peeker.go
    public.go
    scanner.go
    structure.go
    tokentype_string.go
)

GO_TEST_SRCS(
    didyoumean_test.go
    navigation_test.go
    parser_test.go
    public_test.go
    scanner_test.go
    structure_test.go
)

END()

RECURSE(
    fuzz
    gotest
)
