GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    fs.go
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
ENDIF()

END()

RECURSE(
    gotest
)
