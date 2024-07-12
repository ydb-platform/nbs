GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-2-Clause)

SRCS(
    xattr.go
)

IF (OS_LINUX)
    SRCS(
        xattr_linux.go
    )

    GO_TEST_SRCS(
        xattr_flags_test.go
        xattr_linux_test.go
        xattr_test.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        xattr_darwin.go
    )

    GO_TEST_SRCS(
        xattr_flags_test.go
        xattr_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        xattr_unsupported.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
