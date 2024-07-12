GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    map.go
)

GO_XTEST_SRCS(
    map_bench_test.go
    map_reference_test.go
    map_test.go
)

END()

RECURSE(
    gotest
)
