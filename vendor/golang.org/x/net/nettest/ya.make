GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    conntest.go
    nettest.go
)

IF (OS_LINUX)
    SRCS(
        nettest_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        nettest_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        nettest_windows.go
    )
ENDIF()

END()
