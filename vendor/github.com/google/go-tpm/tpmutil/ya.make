GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_SKIP_TESTS(TestEncodingPackTypeInvalid)

SRCS(
    encoding.go
    run.go
    structures.go
)

IF (OS_LINUX)
    SRCS(
        poll_unix.go
        run_other.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        poll_unix.go
        run_other.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        poll_other.go
        run_windows.go
    )
ENDIF()

END()

RECURSE(
    mssim
)

IF (OS_WINDOWS)
    RECURSE(
        tbs
    )
ENDIF()
