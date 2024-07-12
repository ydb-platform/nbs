GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    components.go
    host_wrapper.go
    loggers.go
)

GO_TEST_SRCS(
    components_test.go
    host_wrapper_test.go
)

END()

RECURSE(
    gotest
)
