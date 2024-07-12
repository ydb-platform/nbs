GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    mem_info.go
)

GO_TEST_SRCS(mem_info_test.go)

IF (OS_LINUX)
    SRCS(
        total_memory_linux.go
    )

    GO_TEST_SRCS(total_memory_linux_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        total_memory_other.go
    )

    GO_TEST_SRCS(total_memory_other_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        total_memory_other.go
    )

    GO_TEST_SRCS(total_memory_other_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
