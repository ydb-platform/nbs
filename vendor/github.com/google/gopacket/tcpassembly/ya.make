GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    assembly.go
)

GO_TEST_SRCS(assembly_test.go)

END()

RECURSE(
    gotest
    tcpreader
)
