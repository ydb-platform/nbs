GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    balancers.go
    config.go
)

GO_TEST_SRCS(
    balancers_test.go
    config_test.go
)

END()

RECURSE(
    gotest
)
