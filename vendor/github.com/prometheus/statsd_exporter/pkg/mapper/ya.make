GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    action.go
    escape.go
    mapper.go
    mapper_cache.go
    mapper_defaults.go
    mapping.go
    match.go
    metric_type.go
    observer.go
)

GO_TEST_SRCS(
    escape_test.go
    mapper_benchmark_test.go
    mapper_test.go
)

END()

RECURSE(
    fsm
    gotest
)
