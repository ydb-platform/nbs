GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
)

IF (OS_LINUX)
    SRCS(
        tempfile.go
        writefile.go
    )

    GO_TEST_SRCS(
        symlink_test.go
        tempfile_linux_test.go
        writefile_test.go
    )

    GO_XTEST_SRCS(example_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        tempfile.go
        writefile.go
    )

    GO_TEST_SRCS(
        symlink_test.go
        writefile_test.go
    )

    GO_XTEST_SRCS(example_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
