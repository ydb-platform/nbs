GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    host.go
)

GO_TEST_SRCS(host_test.go)

IF (OS_LINUX)
    SRCS(
        host_linux.go
        host_posix.go
    )

    GO_TEST_SRCS(host_linux_test.go)
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        host_linux_amd64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        host_linux_arm64.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        host_bsd.go
        host_darwin.go
        host_posix.go
        CGO_EXPORT
        smc_darwin.c
    )
ENDIF()

IF (OS_DARWIN AND CGO_ENABLED)
    CGO_SRCS(host_darwin_cgo.go)
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
        host_darwin_amd64.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        host_darwin_arm64.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        host_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
