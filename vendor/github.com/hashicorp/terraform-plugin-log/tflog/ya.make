GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    options.go
    provider.go
    subsystem.go
)

GO_TEST_SRCS(
    provider_example_test.go
    subsystem_example_test.go
)

GO_XTEST_SRCS(
    options_test.go
    provider_test.go
    subsystem_test.go
)

END()

RECURSE(
    gotest
)
