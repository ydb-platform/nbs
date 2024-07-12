GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    options.go
    setup.go
    testinglogger.go
)

GO_XTEST_SRCS(
    contextual_test.go
    example_test.go
    testinglogger_test.go
)

END()

RECURSE(
    example
    gotest
    init
)
