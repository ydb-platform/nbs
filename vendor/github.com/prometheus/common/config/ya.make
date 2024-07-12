GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    http_config.go
)

GO_TEST_SRCS(
    config_test.go
    http_config_test.go
    tls_config_test.go
)

END()

RECURSE(
    gotest
)
