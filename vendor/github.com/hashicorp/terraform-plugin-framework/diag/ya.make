GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    attribute_error_diagnostic.go
    attribute_warning_diagnostic.go
    diagnostic.go
    diagnostics.go
    doc.go
    error_diagnostic.go
    severity.go
    warning_diagnostic.go
    with_path.go
)

GO_XTEST_SRCS(
    diagnostics_test.go
    error_diagnostic_test.go
    warning_diagnostic_test.go
)

END()

RECURSE(
    gotest
)
