GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    agentprovider.go
)

GO_XTEST_SRCS(agentprovider_test.go)

IF (OS_LINUX)
    SRCS(
        agentprovider_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        agentprovider_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        agentprovider_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
