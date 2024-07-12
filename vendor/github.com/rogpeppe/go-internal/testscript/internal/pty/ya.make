GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

IF (OS_LINUX)
    SRCS(
        pty.go
        pty_linux.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        pty.go
        pty_darwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        pty_unsupported.go
    )
ENDIF()

END()
