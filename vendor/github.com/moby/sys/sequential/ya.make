GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
)

IF (OS_LINUX)
    SRCS(
        sequential_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        sequential_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        sequential_windows.go
    )
ENDIF()

END()
