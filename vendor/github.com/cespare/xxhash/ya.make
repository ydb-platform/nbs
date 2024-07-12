GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    rotate19.go
    xxhash.go
    xxhash_unsafe.go
)

GO_TEST_SRCS(xxhash_test.go)

IF (ARCH_X86_64)
    SRCS(
        xxhash_amd64.go
        xxhash_amd64.s
    )

    GO_TEST_SRCS(xxhash_amd64_test.go)
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        xxhash_other.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
