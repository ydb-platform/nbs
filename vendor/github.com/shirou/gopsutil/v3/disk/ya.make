GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    disk.go
)

GO_TEST_SRCS(disk_test.go)

IF (OS_LINUX)
    SRCS(
        disk_linux.go
        disk_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        disk_darwin.go
        disk_unix.go
        CGO_EXPORT
        iostat_darwin.c
    )
ENDIF()

IF (OS_DARWIN AND CGO_ENABLED)
    CGO_SRCS(disk_darwin_cgo.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        disk_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
