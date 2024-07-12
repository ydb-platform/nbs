GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    gcpolicy.go
    load.go
)

GO_TEST_SRCS(load_test.go)

IF (OS_LINUX)
    SRCS(
        gcpolicy_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        gcpolicy_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        gcpolicy_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
