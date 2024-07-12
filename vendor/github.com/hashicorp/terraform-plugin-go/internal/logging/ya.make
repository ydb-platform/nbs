GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    context.go
    doc.go
    environment_variables.go
    keys.go
    protocol.go
    protocol_data.go
    provider.go
)

GO_TEST_SRCS(provider_test.go)

END()

RECURSE(
    gotest
)
