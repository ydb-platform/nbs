GO_TEST()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_XTEST_SRCS(
    code_string_test.go
    context_test.go
    primitives_test.go
    safe_config_selector_test.go
    syncmap_test.go
)

END()
