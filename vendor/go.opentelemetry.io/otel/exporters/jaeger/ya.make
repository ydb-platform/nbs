GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    agent.go
    doc.go
    env.go
    jaeger.go
    reconnecting_udp_client.go
    uploader.go
)

GO_TEST_SRCS(
    agent_test.go
    env_test.go
    jaeger_benchmark_test.go
    jaeger_test.go
    reconnecting_udp_client_test.go
)

IF (OS_LINUX)
    GO_TEST_SRCS(assertsocketbuffersize_test.go)
ENDIF()

IF (OS_DARWIN)
    GO_TEST_SRCS(assertsocketbuffersize_test.go)
ENDIF()

IF (OS_WINDOWS)
    GO_TEST_SRCS(assertsocketbuffersize_windows_test.go)
ENDIF()

END()

RECURSE(
    gotest
    internal
)
