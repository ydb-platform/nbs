GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    format.go
    number.go
    option.go
)

GO_TEST_SRCS(
    format_test.go
    number_test.go
)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    gotest
)
