GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    data_source.go
    diagnostic.go
    doc.go
    dynamic_value.go
    provider.go
    resource.go
    schema.go
    server_capabilities.go
    state.go
    string_kind.go
)

GO_XTEST_SRCS(
    schema_test.go
    state_test.go
)

END()

RECURSE(
    gotest
    internal
    tf6server
)
