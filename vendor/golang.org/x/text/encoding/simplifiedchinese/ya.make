GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    all.go
    gbk.go
    hzgb2312.go
    tables.go
)

GO_TEST_SRCS(all_test.go)

END()

RECURSE(
    gotest
)
