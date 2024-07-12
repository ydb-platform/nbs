GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    execabs.go
    execabs_go119.go
)

GO_TEST_SRCS(execabs_test.go)

END()

RECURSE(
    gotest
)
