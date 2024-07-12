GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    build_info.go
    component.go
    config.go
    doc.go
    host.go
    identifiable.go
    telemetry.go
)

GO_TEST_SRCS(
    component_test.go
    config_test.go
    identifiable_test.go
)

END()

RECURSE(
    componenttest
    gotest
)
