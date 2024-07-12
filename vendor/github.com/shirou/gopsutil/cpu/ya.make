GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    cpu.go
)

GO_TEST_SRCS(cpu_test.go)

IF (OS_LINUX)
    SRCS(
        cpu_linux.go
    )

    GO_TEST_SRCS(cpu_linux_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        cpu_darwin.go
    )
ENDIF()

IF (OS_DARWIN AND CGO_ENABLED)
    CGO_SRCS(cpu_darwin_cgo.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        cpu_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
