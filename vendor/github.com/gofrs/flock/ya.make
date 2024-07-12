GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    flock.go
)

GO_TEST_SRCS(flock_internal_test.go)

GO_XTEST_SRCS(
    flock_example_test.go
    flock_test.go
)

IF (OS_LINUX)
    SRCS(
        flock_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        flock_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        flock_winapi.go
        flock_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
