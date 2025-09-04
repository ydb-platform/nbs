GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.5.0)

SRCS(
    clockwork.go
    context.go
    ticker.go
    timer.go
)

GO_TEST_SRCS(
    clockwork_test.go
    context_test.go
    example_test.go
    ticker_test.go
    timer_test.go
)

END()

RECURSE(
    gotest
)
