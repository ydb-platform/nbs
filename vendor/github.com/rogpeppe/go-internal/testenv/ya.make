GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    testenv.go
    testenv_cgo.go
)

IF (OS_LINUX)
    SRCS(
        testenv_notwin.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        testenv_notwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        testenv_windows.go
    )
ENDIF()

END()
