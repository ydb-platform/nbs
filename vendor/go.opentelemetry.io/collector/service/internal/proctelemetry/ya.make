GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    process_telemetry.go
)

GO_TEST_SRCS(
    config_test.go
    process_telemetry_test.go
)

IF (OS_LINUX)
    GO_TEST_SRCS(process_telemetry_linux_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
