GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    journal.go
)

GO_TEST_SRCS(
    # journal_test.go
)

IF (OS_LINUX)
    SRCS(
        journal_unix.go
    )

    GO_XTEST_SRCS(journal_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        journal_unix.go
    )

    GO_XTEST_SRCS(journal_unix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        journal_windows.go
    )
ENDIF()

END()

RECURSE(
    #gotest
)
