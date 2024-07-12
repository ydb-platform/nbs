GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    strutil.go
)

GO_TEST_SRCS(
    strutil_benchmark_test.go
    strutil_test.go
)

END()

RECURSE(
    gotest
)
