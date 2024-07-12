GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    runewidth.go
    runewidth_table.go
)

GO_TEST_SRCS(
    benchmark_test.go
    runewidth_test.go
)

IF (OS_LINUX)
    SRCS(
        runewidth_posix.go
    )

    GO_TEST_SRCS(runewidth_posix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        runewidth_posix.go
    )

    GO_TEST_SRCS(runewidth_posix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        runewidth_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
