GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    all.go
    eucjp.go
    iso2022jp.go
    shiftjis.go
    tables.go
)

GO_TEST_SRCS(all_test.go)

END()

RECURSE(
    gotest
)
