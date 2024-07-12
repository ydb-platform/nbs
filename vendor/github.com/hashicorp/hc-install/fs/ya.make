GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    any_version.go
    exact_version.go
    fs.go
    version.go
)

GO_TEST_SRCS(
    any_version_test.go
    exact_version_test.go
    fs_test.go
    version_test.go
)

IF (OS_LINUX)
    SRCS(
        fs_unix.go
    )

    GO_TEST_SRCS(fs_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        fs_unix.go
    )

    GO_TEST_SRCS(fs_unix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        fs_windows.go
    )

    GO_TEST_SRCS(fs_windows_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
