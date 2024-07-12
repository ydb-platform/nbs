GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    requires_replace.go
    requires_replace_if.go
    requires_replace_if_configured.go
    requires_replace_if_func.go
    use_state_for_unknown.go
)

GO_XTEST_SRCS(
    requires_replace_if_configured_test.go
    requires_replace_if_test.go
    requires_replace_test.go
    use_state_for_unknown_test.go
)

END()

RECURSE(
    gotest
)
