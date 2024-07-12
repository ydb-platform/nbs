GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    url.go
)

GO_TEST_SRCS(url_test.go)

IF (OS_LINUX)
    SRCS(
        url_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        url_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        url_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
