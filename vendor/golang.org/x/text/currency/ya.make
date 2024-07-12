GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    common.go
    currency.go
    format.go
    query.go
    tables.go
)

GO_TEST_SRCS(
    currency_test.go
    format_test.go
    query_test.go
    tables_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
