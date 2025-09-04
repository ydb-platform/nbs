GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    context.go
    mode.go
    params.go
    response.go
    status.go
    timeout.go
)

GO_TEST_SRCS(params_test.go)

END()

RECURSE(
    gotest
    metadata
    options
)
