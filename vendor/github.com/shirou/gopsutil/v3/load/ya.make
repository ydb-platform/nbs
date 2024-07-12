GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    load.go
)

GO_TEST_SRCS(load_test.go)

IF (OS_LINUX)
    SRCS(
        load_linux.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        load_darwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        load_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
