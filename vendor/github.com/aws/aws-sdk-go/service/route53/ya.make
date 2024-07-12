GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api.go
    customizations.go
    doc.go
    errors.go
    service.go
    unmarshal_error.go
    waiters.go
)

GO_TEST_SRCS(
    unmarshal_error_leak_test.go
    unmarshal_error_test.go
)

GO_XTEST_SRCS(
    customizations_test.go
    examples_test.go
)

END()

RECURSE(
    gotest
    route53iface
)
