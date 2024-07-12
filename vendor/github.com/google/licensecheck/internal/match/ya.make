GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    dict.go
    regexp.go
    rematch.go
    resyntax.go
)

GO_TEST_SRCS(
    dict_test.go
    regexp_test.go
    rematch_test.go
    resyntax_test.go
)

END()

RECURSE(
    gotest
)
