GO_LIBRARY()

SRCS(
    manager.go
    manager_ci.go
)

GO_TEST_SRCS(manager_test.go)

IF (OS_LINUX)
    SRCS(
        manager_linux.go
        manager_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        manager_darwin.go
        manager_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(manager_other.go)
ENDIF()

END()

RECURSE(
    burn_ports
    gotest
)
