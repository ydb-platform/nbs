GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    catalog.go
    dict.go
    go19.go
)

GO_TEST_SRCS(catalog_test.go)

END()

RECURSE(
    gotest
)
