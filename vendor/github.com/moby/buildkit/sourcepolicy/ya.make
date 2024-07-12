GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    engine.go
    formatter.go
    matcher.go
    mutate.go
)

GO_TEST_SRCS(
    engine_test.go
    matcher_test.go
    mutate_test.go
)

END()

RECURSE(
    gotest
    pb
)
