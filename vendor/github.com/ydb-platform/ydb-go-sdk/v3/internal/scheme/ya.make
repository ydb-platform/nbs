GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
    options.go
)

GO_TEST_SRCS(options_test.go)

END()

RECURSE(
    config
    gotest
    helpers
)
