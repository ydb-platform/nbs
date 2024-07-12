GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    host.go
    service.go
    telemetry.go
    zpages.go
)

GO_TEST_SRCS(
    config_test.go
    service_test.go
    telemetry_test.go
)

END()

RECURSE(
    extensions
    gotest
    internal
    pipelines
    telemetry
)
