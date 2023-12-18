GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    context.go
    mode.go
    params.go
    timeout.go
)

GO_TEST_SRCS(params_test.go)

END()

RECURSE(
    gotest
)
