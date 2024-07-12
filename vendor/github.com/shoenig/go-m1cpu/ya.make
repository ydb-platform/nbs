GO_LIBRARY(m1cpu)

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

GO_TEST_SRCS(examples_test.go)

IF (ARCH_X86_64)
    SRCS(
        incompatible.go
    )

    GO_TEST_SRCS(incompatible_test.go)
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        incompatible.go
    )

    GO_TEST_SRCS(incompatible_test.go)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    GO_TEST_SRCS(cpu_test.go)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64 AND CGO_ENABLED)
    CGO_SRCS(cpu.go)
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        incompatible.go
    )

    GO_TEST_SRCS(incompatible_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
