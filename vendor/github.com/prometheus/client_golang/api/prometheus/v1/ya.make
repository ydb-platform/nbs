GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api.go
)

GO_TEST_SRCS(
    api_bench_test.go
    api_test.go
)

GO_XTEST_SRCS(
    # example_test.go
)

END()

RECURSE(
    gotest
)
