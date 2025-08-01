GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    event_broadcast.go
    last_usage_guard.go
    last_usage_guard_start.go
    mutex.go
    once.go
)

GO_TEST_SRCS(
    event_broadcast_test.go
    last_usage_guard_test.go
    mutex_test.go
    once_test.go
)

END()

RECURSE(
    gotest
)
