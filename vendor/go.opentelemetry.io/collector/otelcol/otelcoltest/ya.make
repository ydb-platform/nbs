GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    nop_factories.go
)

GO_TEST_SRCS(
    config_test.go
    nop_factories_test.go
)

END()

RECURSE(
    gotest
)
