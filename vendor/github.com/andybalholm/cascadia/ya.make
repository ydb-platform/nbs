GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-2-Clause)

SRCS(
    parser.go
    pseudo_classes.go
    selector.go
    serialize.go
    specificity.go
)

GO_TEST_SRCS(
    benchmark_test.go
    parser_test.go
    selector_test.go
    serialize_test.go
    specificity_test.go
    w3_test.go
)

END()

RECURSE(
    fuzz
    gotest
)
