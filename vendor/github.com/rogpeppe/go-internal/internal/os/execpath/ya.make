GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    exec.go
)

IF (OS_LINUX)
    SRCS(
        lp_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        lp_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        lp_windows.go
    )
ENDIF()

END()
