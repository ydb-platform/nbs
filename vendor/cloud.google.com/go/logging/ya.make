GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    instrumentation.go
    loggeroption.go
    logging.go
    resource.go
)

GO_TEST_SRCS(
    # logging_unexported_test.go
    # resource_test.go
)

GO_XTEST_SRCS(
    examples_test.go
    # logging_test.go
)

END()

RECURSE(
    apiv2
    gotest
    internal
    logadmin
)
