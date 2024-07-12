GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    config_traversals.go
    contextual.go
    diagnostic.go
    diagnostic_base.go
    diagnostics.go
    doc.go
    error.go
    severity_string.go
    simple_warning.go
)

GO_TEST_SRCS(
    contextual_test.go
    diagnostics_test.go
)

END()

RECURSE(
    gotest
)
