GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    sigv4.go
    sigv4_config.go
)

GO_TEST_SRCS(
    sigv4_config_test.go
    sigv4_test.go
)

END()

RECURSE(
    gotest
)
