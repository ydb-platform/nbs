GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    valid_mac_prefixes.go
)

GO_TEST_SRCS(benchmark_test.go)

END()

RECURSE(
    gotest
)
