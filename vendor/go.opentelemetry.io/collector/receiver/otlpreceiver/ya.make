GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    doc.go
    encoder.go
    factory.go
    otlp.go
    otlphttp.go
)

GO_TEST_SRCS(
    config_test.go
    factory_test.go
    otlp_test.go
)

END()

RECURSE(
    gotest
    internal
)
