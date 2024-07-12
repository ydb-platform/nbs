GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    collate.go
    index.go
    option.go
    sort.go
    tables.go
)

GO_TEST_SRCS(
    collate_test.go
    export_test.go
    option_test.go
    reg_test.go
    table_test.go
)

GO_XTEST_SRCS(
    example_sort_test.go
    examples_test.go
    sort_test.go
)

END()

RECURSE(
    build
    gotest
    tools
)
