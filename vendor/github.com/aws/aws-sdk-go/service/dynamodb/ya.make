GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api.go
    customizations.go
    doc.go
    doc_custom.go
    errors.go
    service.go
    waiters.go
)

GO_XTEST_SRCS(
    cust_example_test.go
    customizations_test.go
    examples_test.go
)

END()

RECURSE(
    gotest
)
