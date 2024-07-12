GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    levels.go
    options.go
    sdk.go
    sink.go
    subsystem.go
)

GO_TEST_SRCS(
    # levels_test.go
    # sdk_example_test.go
    # subsystem_example_test.go
)

GO_XTEST_SRCS(
    # options_test.go
    # sdk_test.go
    # subsystem_test.go
)

END()

RECURSE(
    gotest
)
