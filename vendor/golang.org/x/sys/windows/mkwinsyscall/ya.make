GO_PROGRAM()

LICENSE(BSD-3-Clause)

VERSION(v0.34.0)

SRCS(
    mkwinsyscall.go
)

GO_TEST_SRCS(mkwinsyscall_test.go)

END()

RECURSE(
    gotest
)
