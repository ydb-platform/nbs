GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    galois.go
    inversion_tree.go
    leopard.go
    matrix.go
    options.go
    reedsolomon.go
    streaming.go
)

GO_TEST_SRCS(
    # galois_test.go
    # inversion_tree_test.go
    # leopard_test.go
    # matrix_test.go
    # reedsolomon_test.go
    # streaming_test.go
)

GO_XTEST_SRCS(
    # examples_test.go
)

IF (ARCH_X86_64)
    SRCS(
        galoisAvx512_amd64.go
        galoisAvx512_amd64.s
        galois_amd64.go
        galois_amd64.s
        galois_gen_amd64.go
        galois_gen_amd64.s
        galois_gen_switch_amd64.go
    )

    GO_TEST_SRCS(
        # galoisAvx512_amd64_test.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        galois_arm64.go
        galois_arm64.s
        galois_gen_none.go
        galois_notamd64.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
