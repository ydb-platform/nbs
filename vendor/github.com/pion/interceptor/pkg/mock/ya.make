GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    factory.go
    interceptor.go
)

GO_TEST_SRCS(interceptor_test.go)

END()

RECURSE(
    gotest
)
