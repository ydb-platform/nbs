GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    memory.go
    tcpassembly.go
    tcpcheck.go
)

GO_TEST_SRCS(
    tcpassembly_test.go
    tcpcheck_test.go
)

END()

RECURSE(
    gotest
)
