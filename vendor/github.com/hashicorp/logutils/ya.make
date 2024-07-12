GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    level.go
)

GO_TEST_SRCS(
    level_benchmark_test.go
    level_test.go
)

END()

RECURSE(
    gotest
)
