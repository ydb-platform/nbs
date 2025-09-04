GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    balancers.go
    config.go
    context.go
)

GO_TEST_SRCS(
    balancers_test.go
    config_test.go
)

END()

RECURSE(
    gotest
)
