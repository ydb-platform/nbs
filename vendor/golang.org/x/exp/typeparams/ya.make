GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    common.go
    normalize.go
    termlist.go
    typeparams_go118.go
    typeterm.go
)

GO_XTEST_SRCS(
    common_test.go
    normalize_test.go
    typeparams_test.go
)

END()

RECURSE(
    gotest
)
