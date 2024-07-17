GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    envconfig.go
)

GO_TEST_SRCS(envconfig_test.go)

END()

RECURSE(
    gotest
)
