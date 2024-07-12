GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    addr2liner.go
    addr2liner_llvm.go
    addr2liner_nm.go
    binutils.go
    disasm.go
)

GO_TEST_SRCS(
    binutils_test.go
    disasm_test.go
)

END()

RECURSE(
    # gotest
)
