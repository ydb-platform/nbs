GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    fake_clock.go
    simple_interval_clock.go
)

GO_TEST_SRCS(
    fake_clock_test.go
    simple_interval_clock_test.go
)

END()

RECURSE(
    gotest
)
