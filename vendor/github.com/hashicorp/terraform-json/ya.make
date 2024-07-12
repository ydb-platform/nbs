GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    action.go
    config.go
    expression.go
    metadata.go
    plan.go
    schemas.go
    state.go
    tfjson.go
    validate.go
    version.go
)

GO_TEST_SRCS(
    config_test.go
    expression_test.go
    metadata_test.go
    parse_test.go
    plan_test.go
    schemas_test.go
    state_test.go
    validate_test.go
    version_test.go
)

END()

RECURSE(
    gotest
)
