GO_PROGRAM()

LICENSE(BSD-3-Clause)

SRCS(
    mkwinsyscall.go
)

GO_TEST_SRCS(mkwinsyscall_test.go)

END()

RECURSE(
    gotest
)
