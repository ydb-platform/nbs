GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    otlp.go
)

GO_TEST_SRCS(otlp_test.go)

END()

RECURSE(
    gotest
)
