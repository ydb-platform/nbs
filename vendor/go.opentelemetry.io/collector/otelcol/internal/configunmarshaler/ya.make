GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    configs.go
)

GO_TEST_SRCS(configs_test.go)

END()

RECURSE(
    gotest
)
