GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    attribute_path.go
    data_source.go
    diagnostic.go
    dynamic_value.go
    provider.go
    resource.go
    schema.go
    server_capabilities.go
    state.go
    string_kind.go
)

GO_TEST_SRCS(diagnostic_test.go)

END()

RECURSE(
    gotest
)
