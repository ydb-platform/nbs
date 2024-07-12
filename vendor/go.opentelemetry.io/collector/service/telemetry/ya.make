GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    generated_config.go
    otel_trace_sampler.go
    telemetry.go
)

GO_TEST_SRCS(config_test.go)

END()

RECURSE(
    gotest
)
