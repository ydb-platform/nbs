GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    defaults.go
    shared_config.go
)

GO_TEST_SRCS(defaults_test.go)

END()

RECURSE(gotest)