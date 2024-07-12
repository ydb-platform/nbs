GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    context.go
    environment_variables.go
    helper_resource.go
    helper_schema.go
    keys.go
)

GO_XTEST_SRCS(
    context_test.go
    helper_resource_test.go
    helper_schema_test.go
)

END()

RECURSE(
    gotest
)
